package s3router

import "net/http"

// Customizer is called once per leg ("primary", "secondary", …).
// op     – canonical S3 operation ("GetObject", "PutObject" …)
// epName – logical endpoint name coming from the config ("primary" / "secondary" …)
// rule   – the Rule that matched (lets the hook know bucket/prefix)
// Both methods are optional; supply nil if not needed.
type Customizer interface {
	Before(req *http.Request, op string, epName string, rule Rule)
	After(resp *http.Response, op string, epName string, rule Rule) error
}

// customizers holds registered Customizer instances per endpoint.
var customizers = make(map[string]Customizer)

// RegisterCustomizer registers a Customizer for the given endpoint name.
// Supply nil to clear an existing customizer.
func RegisterCustomizer(endpointName string, c Customizer) {
	customizers[endpointName] = c
}
