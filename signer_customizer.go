package s3router

import (
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// v4Customizer implements Customizer to SigV4-sign each request for S3.
type v4Customizer struct {
	creds  aws.CredentialsProvider
	region string
}

// Before removes any existing Authorization header and signs the request in-place.
func (c v4Customizer) Before(req *http.Request, op string, epName string, rule Rule) {
	// drop any prior AWS signature
	req.Header.Del("Authorization")
	// sign the HTTP request for S3
	signer := v4.NewSigner()
	_ = signer.SignHTTP(
		req.Context(),
		c.creds,
		req,
		req.Body,
		"s3",
		c.region,
		time.Now(),
	)
}

// After is a no-op for v4 signing.
func (c v4Customizer) After(resp *http.Response, op string, epName string, rule Rule) error {
	return nil
}

// NewWithAWSCreds wraps New(...) and registers a v4Customizer for each endpoint in creds.
// creds maps endpoint names to AWS CredentialProviders, all using the same region.
func NewWithAWSCreds(
	cfg *Config,
	creds map[string]aws.CredentialsProvider,
	region string,
) (http.RoundTripper, error) {
	rt, err := New(cfg)
	if err != nil {
		return nil, err
	}
	for ep, cp := range creds {
		RegisterCustomizer(ep, v4Customizer{creds: cp, region: region})
	}
	return rt, nil
}
