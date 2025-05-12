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

// customizers holds registered Customizer instances per endpoint; may be a composite.
var customizers = make(map[string]Customizer)

// multiCustomizer wraps multiple Customizers into one.
type multiCustomizer []Customizer

func (m multiCustomizer) Before(req *http.Request, op string, epName string, rule Rule) {
	for _, c := range m {
		c.Before(req, op, epName, rule)
	}
}

func (m multiCustomizer) After(resp *http.Response, op string, epName string, rule Rule) error {
	var err error
	for _, c := range m {
		if e := c.After(resp, op, epName, rule); err == nil && e != nil {
			err = e
		}
	}
	return err
}

// RegisterCustomizer registers a Customizer for the given endpoint name.
// Supply nil to clear existing customizers
func RegisterCustomizer(endpointName string, c Customizer) {
	if c == nil {
		delete(customizers, endpointName)
		return
	}
	if existing, ok := customizers[endpointName]; ok {
		// combine existing and new into a multiCustomizer
		if mc, ok2 := existing.(multiCustomizer); ok2 {
			customizers[endpointName] = append(mc, c)
		} else {
			customizers[endpointName] = multiCustomizer{existing, c}
		}
	} else {
		customizers[endpointName] = c
	}
}
