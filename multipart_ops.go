package s3router

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/store"
)

func (c *router) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	const op = "CreateMultipartUpload"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, st store.Store, in *s3.CreateMultipartUploadInput) (*s3.CreateMultipartUploadOutput, error) {
			return st.CreateMultipartUpload(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *router) UploadPart(ctx context.Context, in *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	const op = "UploadPart"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, st store.Store, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
			return st.UploadPart(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *router) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	const op = "CompleteMultipartUpload"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, st store.Store, in *s3.CompleteMultipartUploadInput) (*s3.CompleteMultipartUploadOutput, error) {
			return st.CompleteMultipartUpload(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *router) ListParts(ctx context.Context, in *s3.ListPartsInput, optFns ...func(*s3.Options)) (*s3.ListPartsOutput, error) {
	const op = "ListParts"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, st store.Store, in *s3.ListPartsInput) (*s3.ListPartsOutput, error) {
			return st.ListParts(ctx, in)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}

func (c *router) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	const op = "AbortMultipartUpload"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	action, err := c.routeAction(op, bucket, key)
	if err != nil {
		return nil, err
	}
	primB, secB := c.cfg.PhysicalBuckets(bucket)
	inPrimary, inSecondary := *in, *in
	inPrimary.Bucket, inSecondary.Bucket = aws.String(primB), aws.String(secB)
	return dispatch(ctx, action,
		func(ctx context.Context, st store.Store, in *s3.AbortMultipartUploadInput) (*s3.AbortMultipartUploadOutput, error) {
			return st.AbortMultipartUpload(ctx, in, optFns...)
		},
		&inPrimary, &inSecondary,
		c.primary, c.secondary,
	)
}
