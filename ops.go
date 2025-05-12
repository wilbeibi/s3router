package s3router

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
)

func (c *client) routeAction(op, bucket, key string) (config.Action, error) {
	if !c.cfg.IsLogicalBucket(bucket) {
		return "", fmt.Errorf("%s: bucket %q is not configured", op, bucket)
	}
	_, action := c.cfg.Lookup(bucket, key, op)
	return action, nil
}

func (c *client) GetObject(
	ctx context.Context,
	in *s3.GetObjectInput,
	optFns ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	const op = "GetObject"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, cl *s3.Client, in *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			return cl.GetObject(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *client) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	const op = "PutObject"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
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
	return dispatch(ctx, action,
		func(ctx context.Context, cl *s3.Client, in *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
			return cl.PutObject(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *client) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	const op = "HeadObject"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, cl *s3.Client, in *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return cl.HeadObject(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *client) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	const op = "DeleteObject"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, cl *s3.Client, in *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
			return cl.DeleteObject(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *client) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	const op = "ListObjectsV2"
	bucket := aws.ToString(in.Bucket)
	action, err := c.routeAction(op, bucket, "")
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, cl *s3.Client, in *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
			return cl.ListObjectsV2(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}
