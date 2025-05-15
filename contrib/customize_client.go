package my_customize_client

import (
	"bytes"
	"context"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/store"
)

// MyCustomizeClient wraps an S3 client and injects content-length behavior when missing.
type MyCustomizeClient struct{ *s3.Client }

var _ store.Store = (*MyCustomizeClient)(nil)

func NewMyCustomizeClient(client *s3.Client) *MyCustomizeClient {
	return &MyCustomizeClient{client}
}

// PutObject overrides the embedded PutObject to set ContentLength when missing.
func (s *MyCustomizeClient) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if in.ContentLength == nil && in.Body != nil {
		if seeker, ok := in.Body.(io.Seeker); ok {
			cur, _ := seeker.Seek(0, io.SeekCurrent)
			end, _ := seeker.Seek(0, io.SeekEnd)
			size := end - cur
			_, _ = seeker.Seek(cur, io.SeekStart)
			in.ContentLength = aws.Int64(size)
		} else {
			data, err := io.ReadAll(in.Body)
			if err != nil {
				return nil, err
			}
			in.ContentLength = aws.Int64(int64(len(data)))
			in.Body = bytes.NewReader(data)
		}
	}
	return s.Client.PutObject(ctx, in, optFns...)
}
