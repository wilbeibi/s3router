package s3router

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
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

	switch action {
	case config.ActPrimary:
		return c.primary.UploadPart(ctx, &inPrimary, optFns...)
	case config.ActSecondary:
		return c.secondary.UploadPart(ctx, &inSecondary, optFns...)
	case config.ActMirror:
		if in.Body != nil {
			var (
				r1, r2 io.Reader
				err    error
			)
			// UploadPart typically handles large chunks (5MB-5GB), use streaming.
			if in.ContentLength == nil || *in.ContentLength >= c.maxBufferBytes {
				r1, r2, err = teeBody(ctx, in.Body)
			} else {
				r1, r2, err = drainBodyLimited(ctx, in.Body, c.maxBufferBytes)
			}
			if err != nil {
				return nil, fmt.Errorf("%s: failed to split body for mirror: %w", op, err)
			}
			inPrimary.Body = r1
			inSecondary.Body = r2
		}
		return dispatch(ctx, action,
			func(ctx context.Context, st store.Store, in *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
				return st.UploadPart(ctx, in, optFns...)
			},
			&inPrimary, &inSecondary,
			c.primary, c.secondary,
		)
	case config.ActBestEffort:
		if in.Body != nil {
			rs, ok := in.Body.(io.ReadSeeker)
			if !ok {
				return nil, fmt.Errorf("%s: best-effort requires Body to be io.ReadSeeker", op)
			}
			start, err := rs.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("%s: failed to seek body: %w", op, err)
			}
			inPrimary.Body = rs
			out, err := c.primary.UploadPart(ctx, &inPrimary, optFns...)
			if _, serr := rs.Seek(start, io.SeekStart); serr != nil {
				if err == nil {
					return out, nil
				}
				return nil, fmt.Errorf("%s: failed to seek body for best-effort: %w", op, serr)
			}
			inSecondary.Body = rs
			out2, err2 := c.secondary.UploadPart(ctx, &inSecondary, optFns...)
			if err == nil {
				return out, nil
			}
			return out2, err2
		}
		out, err := c.primary.UploadPart(ctx, &inPrimary, optFns...)
		out2, err2 := c.secondary.UploadPart(ctx, &inSecondary, optFns...)
		if err == nil {
			return out, nil
		}
		return out2, err2
	case config.ActFallback:
		if in.Body != nil {
			rs, ok := in.Body.(io.ReadSeeker)
			if !ok {
				return nil, fmt.Errorf("%s: fallback requires Body to be io.ReadSeeker", op)
			}
			start, err := rs.Seek(0, io.SeekCurrent)
			if err != nil {
				return nil, fmt.Errorf("%s: failed to seek body: %w", op, err)
			}
			inPrimary.Body = rs
			out, err := c.primary.UploadPart(ctx, &inPrimary, optFns...)
			if err == nil {
				return out, nil
			}
			if _, serr := rs.Seek(start, io.SeekStart); serr != nil {
				return nil, fmt.Errorf("%s: failed to seek body for fallback: %w", op, serr)
			}
			inSecondary.Body = rs
			return c.secondary.UploadPart(ctx, &inSecondary, optFns...)
		}
		out, err := c.primary.UploadPart(ctx, &inPrimary, optFns...)
		if err == nil {
			return out, nil
		}
		return c.secondary.UploadPart(ctx, &inSecondary, optFns...)
	default:
		// Fall back to primary if action is unknown
		return c.primary.UploadPart(ctx, &inPrimary, optFns...)
	}
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
