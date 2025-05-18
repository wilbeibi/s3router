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

func ensureContentLength(in *s3.PutObjectInput) error {
	if in.Body == nil || in.ContentLength != nil {
		return nil
	}

	if rs, ok := in.Body.(io.ReadSeeker); ok {
		cur, err := rs.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		defer rs.Seek(cur, io.SeekStart)

		end, err := rs.Seek(0, io.SeekEnd)
		if err != nil {
			return err
		}

		in.ContentLength = aws.Int64(end - cur)
		return nil
	}

	data, err := io.ReadAll(in.Body)
	if err != nil {
		return err
	}
	in.ContentLength = aws.Int64(int64(len(data)))
	in.Body = bytes.NewReader(data)
	return nil
}

// PutObject overrides the embedded PutObject to set ContentLength when missing.
func (s *MyCustomizeClient) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if err := ensureContentLength(in); err != nil {
		return nil, err
	}
	return s.Client.PutObject(ctx, in, optFns...)
}
