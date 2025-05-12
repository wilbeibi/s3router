package s3router

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
)

func (c *client) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	const op = "PutObject"
	bucket := aws.ToString(in.Bucket)
	key := aws.ToString(in.Key)

	fmt.Printf("[debug] %s: bucket=%s, key=%s\n", op, bucket, key)

	// validate logical bucket
	if !c.cfg.IsLogicalBucket(bucket) {
		return nil, fmt.Errorf("%s: bucket %q is not configured", op, bucket)
	}

	// determine routing action
	_, action := c.cfg.Lookup(bucket, key, op)

	// clone input for each endpoint
	// TODO: a customizer Before() to tweak input based on endpoint
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket = aws.String(c.cfg.GetPhysicalBucket(bucket, "primary"))
	inSecondary.Bucket = aws.String(c.cfg.GetPhysicalBucket(bucket, "secondary"))

	// for mirror actions, split or tee the body
	if action == config.ActMirror && in.Body != nil {
		var (
			r1, r2 io.Reader
			err    error
		)
		if in.ContentLength != nil && *in.ContentLength < c.maxBufferBytes {
			r1, r2, err = drainBody(ctx, in.Body)
		} else {
			r1, r2, err = teeBody(ctx, in.Body)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: failed to split body for mirror: %w", op, err)
		}
		inPrimary.Body = r1
		inSecondary.Body = r2
	}

	// define endpoint functions
	primaryFn := func(ctx context.Context, cl *s3.Client) (*s3.PutObjectOutput, error) {
		return cl.PutObject(ctx, &inPrimary, optFns...)
	}
	secondaryFn := func(ctx context.Context, cl *s3.Client) (*s3.PutObjectOutput, error) {
		return cl.PutObject(ctx, &inSecondary, optFns...)
	}

	// dispatch according to action
	return dispatch(ctx, action, primaryFn, secondaryFn, c.primary, c.secondary)
}
