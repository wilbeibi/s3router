package contrib

import (
	"net/http"

	"github.com/wilbeibi/s3router"
)

// WARNING: This is a demo customizer, the real MinIO might behave differently.
// MinioCustomizer is an example Customizer for MinIO backends.
type MinioCustomizer struct{}

// Before tweaks the request before it's sent to MinIO.
// It adds the UNSIGNED-PAYLOAD header and drops Content-MD5.
func (MinioCustomizer) Before(r *http.Request, op string, epName string, rule s3router.Rule) {
	r.Header.Set("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
	r.Header.Del("Content-MD5")
}

// After does nothing for this example.
func (MinioCustomizer) After(resp *http.Response, op string, epName string, rule s3router.Rule) error {
	return nil
}
