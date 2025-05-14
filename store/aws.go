package store

import "github.com/aws/aws-sdk-go-v2/service/s3"

// AWSStore is a thin wrapper around the AWS S3 client.
type AWSStore struct{ *s3.Client }

var _ Store = (*AWSStore)(nil)

// NewAWSStore creates a new AWSStore.
func NewAWSStore(client *s3.Client) *AWSStore {
	return &AWSStore{client}
}
