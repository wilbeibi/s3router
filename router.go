package s3router

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
)

type Option func(*ruleRT)

func WithMaxBufferBytes(n int64) Option {
	return func(rt *ruleRT) {
		rt.maxBufferBytes = n
	}
}

func New(cfg *Config, opts ...Option) (http.RoundTripper, error) {
	rt := &ruleRT{
		cfg:            cfg,
		endpoints:      map[string]*url.URL{},
		tPrimary:       http.DefaultTransport,
		maxBufferBytes: 256 << 20, // default 256 MiB
	}
	for _, o := range opts {
		o(rt)
	}
	for name, raw := range cfg.Endpoints {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		rt.endpoints[name] = u
	}
	return rt, nil
}

type ruleRT struct {
	cfg            *Config
	endpoints      map[string]*url.URL
	tPrimary       http.RoundTripper
	maxBufferBytes int64 // threshold for in-memory buffering vs streaming
}

func (rt *ruleRT) RoundTrip(req *http.Request) (*http.Response, error) {
	bucket, key := parseS3Path(req)
	op := s3op(req)

	rule, action, found := rt.lookupAction(bucket, key, op)
	if !found {
		// no rule -> primary (apply wildcard alias if any) and invoke hooks
		wildcardRule, _, _ := rt.lookupAction("*", key, op)
		return rt.sendOnce("primary", wildcardRule, req, nil)
	}

	switch action {
	case actPrimary:
		return rt.sendOnce("primary", rule, req, nil)
	case actSecondary:
		return rt.sendOnce("secondary", rule, req, nil)
	case actFallback:
		return rt.doFallback(req, rule)
	case actBestEffort:
		return rt.doDual(req, rule, false)
	case actMirror:
		return rt.doDual(req, rule, true)
	default:
		return nil, fmt.Errorf("internal error: unknown action %q from lookup", action)
	}
}

// lookupAction finds the best matching rule and the effective action for the request.
func (rt *ruleRT) lookupAction(bucket, key, op string) (Rule, action, bool) {
	for _, r := range rt.cfg.Rules {
		if r.Bucket != bucket && r.Bucket != "*" {
			continue
		}
		if r.Prefix != "*" && r.Prefix != "" && !strings.HasPrefix(key, r.Prefix) {
			continue
		}

		// Found a matching rule (due to sorting, it's the most specific one)
		if act, ok := r.ActionFor[op]; ok {
			return r, act, true // Exact op match
		}
		if act, ok := r.ActionFor["*"]; ok {
			return r, act, true // Fallback to wildcard op
		}
		return Rule{}, "", false
	}
	return Rule{}, "", false
}

// applyAlias modifies the request URL (path-style or virtual-host) to use
// the bucket alias for the given endpoint, if one exists in the rule.
func applyAlias(r *http.Request, rule Rule, ep string) {
	if rule.Alias == nil {
		return
	}
	newName, ok := rule.Alias[ep]
	if !ok || newName == "" || newName == rule.Bucket {
		return // No alias for this endpoint, or it's the same as canonical
	}

	// path-style  /bucket/key...
	if p := strings.TrimPrefix(r.URL.Path, "/"); strings.HasPrefix(p, rule.Bucket+"/") || p == rule.Bucket {
		r.URL.Path = "/" + newName + strings.TrimPrefix(p, rule.Bucket)
		return
	}

	// virtual-host  bucket.s3.amazonaws.com
	if h := r.URL.Hostname(); strings.HasPrefix(h, rule.Bucket+".") {
		r.URL.Host = strings.Replace(h, rule.Bucket+".", newName+".", 1)
	}
}

func (rt *ruleRT) doFallback(src *http.Request, rule Rule) (*http.Response, error) {
	secEpName := "secondary"

	// Attempt 1: Primary with hooks
	resp, err := rt.sendOnce("primary", rule, src, nil)

	if err == nil && resp.StatusCode < 400 {
		return resp, nil
	}
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}

	// Attempt 2: Secondary with hooks
	return rt.sendOnce(secEpName, rule, src, nil)
}

func (rt *ruleRT) doDual(src *http.Request, rule Rule, strong bool) (*http.Response, error) {
	secEpName := "secondary"

	// choose streaming for large bodies (>maxBufferBytes), else buffer in memory
	var b1, b2 io.ReadCloser
	var err error
	if src.ContentLength > rt.maxBufferBytes {
		b1, b2, err = teeBody(src)
	} else {
		b1, b2, err = drainBody(src)
	}
	if err != nil {
		return nil, err
	}

	// Dispatch both requests with hooks concurrently
	c := make(chan result, 2)
	go func() {
		resp, err := rt.sendOnce("primary", rule, src, b1)
		c <- result{resp, err}
	}()
	go func() {
		resp, err := rt.sendOnce(secEpName, rule, src, b2)
		c <- result{resp, err}
	}()

	resA := <-c
	resB := <-c

	// choose primary's view
	if resA.err == nil && resA.resp.StatusCode < 500 {
		if strong && (resB.err != nil || resB.resp.StatusCode >= 500) {
			if resA.resp != nil && resA.resp.Body != nil {
				resA.resp.Body.Close()
			}
			return nil, resB.err
		}
		// Close secondary response body if not needed
		if resB.resp != nil && resB.resp.Body != nil {
			resB.resp.Body.Close()
		}
		return resA.resp, nil
	}
	if !strong && resB.err == nil && resB.resp.StatusCode < 500 {
		// Close primary response body if using secondary
		if resA.resp != nil && resA.resp.Body != nil {
			resA.resp.Body.Close()
		}
		return resB.resp, nil
	}

	if resA.resp != nil && resA.resp.Body != nil {
		resA.resp.Body.Close()
	}
	if resB.resp != nil && resB.resp.Body != nil {
		resB.resp.Body.Close()
	}
	return nil, resA.err
}

type result struct {
	resp *http.Response
	err  error
}

func send(t http.RoundTripper, req *http.Request, ch chan<- result) {
	resp, err := t.RoundTrip(req)
	ch <- result{resp, err}
}

func clone(r *http.Request, body io.ReadCloser) *http.Request {
	r2 := r.Clone(r.Context())
	r2.Body = body
	if r.GetBody != nil {
		r2.GetBody = func() (io.ReadCloser, error) { return body, nil }
	}
	return r2
}

func rewrite(r *http.Request, endpoint *url.URL) *http.Request {
	u := *r.URL
	u.Scheme = endpoint.Scheme
	u.Host = endpoint.Host
	if ep := strings.TrimRight(endpoint.Path, "/"); ep != "" {
		u.Path = path.Join(ep, u.Path)
	}
	r.URL = &u
	return r
}

func drainBody(r *http.Request) (io.ReadCloser, io.ReadCloser, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return http.NoBody, http.NoBody, nil
	}
	defer r.Body.Close()
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, nil, err
	}
	return io.NopCloser(bytes.NewReader(b)), io.NopCloser(bytes.NewReader(b)), nil
}

// teeBody duplicates the request body across two readers via pipes,
// streaming without buffering the full content.
func teeBody(r *http.Request) (io.ReadCloser, io.ReadCloser, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return http.NoBody, http.NoBody, nil
	}
	pr1, pw1 := io.Pipe()
	pr2, pw2 := io.Pipe()
	mw := io.MultiWriter(pw1, pw2)
	go func() {
		defer r.Body.Close()
		if _, err := io.Copy(mw, r.Body); err != nil {
			pw1.CloseWithError(err)
			pw2.CloseWithError(err)
		} else {
			pw1.Close()
			pw2.Close()
		}
	}()
	return pr1, pr2, nil
}

// s3op maps (method, path, query) → canonical S3 operation string.
func s3op(r *http.Request) string {
	q := r.URL.Query()
	switch r.Method {
	case http.MethodPut:
		switch {
		case q.Has("uploadId") && q.Has("partNumber"):
			return "UploadPart"
		case q.Has("uploadId"):
			return "CompleteMultipartUpload"
		case strings.HasSuffix(r.URL.Path, "?acl"):
			return "PutBucketAcl" // very rare
		default:
			return "PutObject"
		}
	case http.MethodGet:
		switch {
		case q.Get("list-type") == "2":
			return "ListObjectsV2"
		case q.Get("uploads") == "":
			return "ListMultipartUploads"
		case q.Has("partNumber") && q.Has("uploadId"):
			return "GetObject"
		default:
			return "GetObject"
		}
	case http.MethodDelete:
		if q.Has("uploadId") {
			return "AbortMultipartUpload"
		}
		return "DeleteObject"
	case http.MethodHead:
		return "HeadObject"
	}
	return ""
}

func parseS3Path(r *http.Request) (bucket, key string) {
	// path‑style:  /bucket/key/…
	p := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if p != "" {
		if parts := strings.SplitN(p, "/", 2); len(parts) > 0 {
			bucket = parts[0]
			if len(parts) == 2 {
				key = parts[1]
			}
		}
	}
	// virtual‑host style:  bucket.s3.amazonaws.com/…
	if bucket == "" {
		if host := r.URL.Hostname(); host != "" {
			bucket = strings.Split(host, ".")[0] // first label
		}
	}
	return bucket, key
}

// sendOnce executes a single request leg to the specified endpoint with optional hooks.
// dstEp is the endpoint name ("primary"/"secondary"), src is the original request, body overrides the request body (nil to use default).
func (rt *ruleRT) sendOnce(dstEp string, rule Rule, src *http.Request, body io.ReadCloser) (*http.Response, error) {
	op := s3op(src)
	// retrieve the customizer (single or composite)
	c := customizers[dstEp]
	req := clone(src, body)
	req = rewrite(req, rt.endpoints[dstEp])
	applyAlias(req, rule, dstEp)
	if c != nil {
		c.Before(req, op, dstEp, rule)
	}
	resp, err := rt.tPrimary.RoundTrip(req)
	if err == nil && c != nil {
		err = c.After(resp, op, dstEp, rule)
	}
	return resp, err
}
