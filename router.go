package s3router

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
)

func New(cfg *Config) (http.RoundTripper, error) {
	rt := &ruleRT{
		cfg:       cfg,
		endpoints: map[string]*url.URL{},
		tPrimary:  http.DefaultTransport,
	}
	// parse endpoint URLs once
	for name, raw := range cfg.Endpoints {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, err
		}
		rt.endpoints[name] = u
	}
	rt.sorted = map[string][]string{} // bucket → sorted prefixes
	for b, p := range cfg.Rules {
		rt.sorted[b] = p.sortedPrefixes()
	}
	return rt, nil
}

// --------------------------------------------------------------------
// Transport
// --------------------------------------------------------------------

type ruleRT struct {
	cfg       *Config
	endpoints map[string]*url.URL
	sorted    map[string][]string // bucket → prefixes sorted by length

	tPrimary http.RoundTripper
}

func (rt *ruleRT) RoundTrip(req *http.Request) (*http.Response, error) {
	bucket, key := parseS3Path(req)
	op := s3op(req)

	action, sec, found := rt.lookup(bucket, key, op)
	if !found {
		// no rule → primary
		return rt.tPrimary.RoundTrip(rewrite(req, rt.endpoints["primary"]))
	}

	switch action {
	case actPrimary:
		return rt.tPrimary.RoundTrip(rewrite(req, rt.endpoints["primary"]))

	case actSecondary:
		return rt.tPrimary.RoundTrip(rewrite(req, rt.endpoints[sec]))

	case actFallback:
		return rt.doFallback(req, sec)

	case actBestEffort:
		return rt.doDual(req, sec, false)

	case actMirror:
		return rt.doDual(req, sec, true)

	default:
		return nil, errors.New("unknown action " + string(action))
	}
}

// --------------------------------------------------------------------
// Routing lookup
// --------------------------------------------------------------------

func (rt *ruleRT) lookup(bucket, key, op string) (action, string, bool) {
	prefMap, ok := rt.cfg.Rules[bucket]
	if !ok {
		// try /* rule
		prefMap, ok = rt.cfg.Rules["*"]
		if !ok {
			return "", "", false
		}
	}

	// longest‑prefix‑wins
	for _, pref := range rt.sorted[bucket] {
		if pref != "*" && !strings.HasPrefix(key, pref) {
			continue
		}
		ops := prefMap[pref]
		if a, ok := ops[op]; ok {
			return a, rt.pickSecondary(a), true
		}
		return ops["*"], rt.pickSecondary(ops["*"]), true
	}
	return "", "", false
}

// pick secondary name from action token
func (rt *ruleRT) pickSecondary(a action) string {
	switch a {
	case actSecondary:
		return "secondary"
	case actMirror, actBestEffort, actFallback:
		return "secondary" // hard‑coded per example; extend if multiple
	default:
		return ""
	}
}

// --------------------------------------------------------------------
// Action handlers
// --------------------------------------------------------------------

func (rt *ruleRT) doFallback(src *http.Request, sec string) (*http.Response, error) {
	resp, err := rt.tPrimary.RoundTrip(rewrite(src, rt.endpoints["primary"]))
	if err == nil && resp.StatusCode < 500 {
		return resp, nil
	}
	return rt.tPrimary.RoundTrip(rewrite(src, rt.endpoints[sec]))
}

func (rt *ruleRT) doDual(src *http.Request, sec string, strong bool) (*http.Response, error) {
	p1 := rt.endpoints["primary"]
	p2 := rt.endpoints[sec]

	b1, b2, err := drainBody(src)
	if err != nil {
		return nil, err
	}

	req1 := rewrite(clone(src, b1), p1)
	req2 := rewrite(clone(src, b2), p2)

	c := make(chan result, 2)
	go send(rt.tPrimary, req1, c)
	go send(rt.tPrimary, req2, c)

	resA := <-c
	resB := <-c

	// choose primary's view
	if resA.err == nil && resA.resp.StatusCode < 500 {
		if strong && (resB.err != nil || resB.resp.StatusCode >= 500) {
			_ = resA.resp.Body.Close()
			return nil, resB.err
		}
		return resA.resp, nil
	}
	if !strong && resB.err == nil && resB.resp.StatusCode < 500 {
		return resB.resp, nil
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
	// --- path‑style:  /bucket/key/…  --------------------------------
	p := strings.TrimPrefix(r.URL.EscapedPath(), "/")
	if p != "" {
		if parts := strings.SplitN(p, "/", 2); len(parts) > 0 {
			bucket = parts[0]
			if len(parts) == 2 {
				key = parts[1]
			}
		}
	}
	// --- virtual‑host style:  bucket.s3.amazonaws.com/…  ------------
	if bucket == "" {
		if host := r.URL.Hostname(); host != "" {
			bucket = strings.Split(host, ".")[0] // first label
		}
	}
	return bucket, key
}
