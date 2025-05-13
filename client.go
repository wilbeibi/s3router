/*
flowchart TD
    subgraph User Code
        U[svc.GetObject(...)]
    end
    U -->|1| W[wrapper GetObject]
    W -->|2| route
    route -->|3a choose| lookup
    lookup --> route
    route -->|3b dispatch| doSerial & doParallel
    doSerial -->|4| primaryFn & secondaryFn
    doParallel -->|4| primaryFn & secondaryFn
    primaryFn -->|5| primarySDK[s3.Client.GetObject]
    secondaryFn -->|5| secondarySDK[s3.Client.GetObject]
*/

package s3router

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
)

type Client interface {
	GetObject(ctx context.Context, in *s3.GetObjectInput,
		optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput,
		optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, in *s3.HeadObjectInput,
		optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, in *s3.DeleteObjectInput,
		optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input,
		optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput,
		optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error)
	UploadPart(ctx context.Context, in *s3.UploadPartInput,
		optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error)
	CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput,
		optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error)
	ListParts(ctx context.Context, in *s3.ListPartsInput,
		optFns ...func(*s3.Options)) (*s3.ListPartsOutput, error)
	AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput,
		optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error)
}

// Option configures the client.
type Option func(*client)

// WithMaxBufferBytes sets the streaming buffer size.
func WithMaxBufferBytes(n int64) Option {
	return func(c *client) {
		c.maxBufferBytes = n
	}
}

// New builds the facade around two pre-configured SDK clients.
func New(cfg *config.Config,
	primary, secondary *s3.Client,
	opts ...Option) (Client, error) {
	c := &client{
		cfg:            cfg,
		primary:        primary,
		secondary:      secondary,
		maxBufferBytes: 256 << 20,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

type client struct {
	cfg            *config.Config
	primary        *s3.Client
	secondary      *s3.Client
	maxBufferBytes int64 // 256 MiB default
}

// Serial "primary-then-secondary if needed" (fallback).
func doSerial[I any, T any](
	ctx context.Context,
	op func(context.Context, *s3.Client, I) (T, error),
	in1, in2 I,
	c1, c2 *s3.Client,
) (T, error) {
	out, err := op(ctx, c1, in1)
	if err == nil {
		return out, nil
	}
	return op(ctx, c2, in2)
}

// Parallel dual-write/read. strict==true => mirror; false => best-effort.
func doParallel[I any, T any](
	ctx context.Context,
	strict bool,
	op func(context.Context, *s3.Client, I) (T, error),
	in1, in2 I,
	c1, c2 *s3.Client,
) (T, error) {
	if strict {
		var wg sync.WaitGroup
		var out T
		var errA, errB error
		wg.Add(2)
		go func() {
			defer wg.Done()
			out, errA = op(ctx, c1, in1)
		}()
		go func() {
			defer wg.Done()
			_, errB = op(ctx, c2, in2)
		}()
		wg.Wait()
		if errA != nil {
			var zero T
			return zero, errA
		}
		if errB != nil {
			var zero T
			return zero, errB
		}
		return out, nil
	}
	// best-effort: fire-and-forget secondary
	out, err := op(ctx, c1, in1)
	go func() {
		_, _ = op(ctx, c2, in2)
	}()
	return out, err
}

func drainBody(ctx context.Context, r io.Reader) (io.ReadSeeker, io.ReadSeeker, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}
	return bytes.NewReader(data), bytes.NewReader(data), nil
}

func teeBody(ctx context.Context, r io.Reader) (io.ReadCloser, io.ReadCloser, error) {
	pr1, pw1 := io.Pipe()
	pr2, pw2 := io.Pipe()
	go func() {
		defer pw1.Close()
		defer pw2.Close()

		select {
		case <-ctx.Done():
			err := ctx.Err()
			pw1.CloseWithError(err)
			pw2.CloseWithError(err)
			return
		default:
			_, err := io.Copy(io.MultiWriter(pw1, pw2), r)
			if err != nil {
				pw1.CloseWithError(err)
				pw2.CloseWithError(err)
			}
		}
	}()

	return pr1, pr2, nil
}

// dispatch executes the primary and secondary functions according to action.
func dispatch[I any, T any](
	ctx context.Context,
	action config.Action,
	op func(context.Context, *s3.Client, I) (T, error),
	primaryInput, secondaryInput I,
	c1, c2 *s3.Client,
) (T, error) {
	switch action {
	case config.ActPrimary:
		return op(ctx, c1, primaryInput)
	case config.ActSecondary:
		return op(ctx, c2, secondaryInput)
	case config.ActFallback:
		return doSerial(ctx, op, primaryInput, secondaryInput, c1, c2)
	case config.ActBestEffort:
		return doParallel(ctx, false, op, primaryInput, secondaryInput, c1, c2)
	case config.ActMirror:
		return doParallel(ctx, true, op, primaryInput, secondaryInput, c1, c2)
	default:
		// Fall back to primary if action is unknown
		return op(ctx, c1, primaryInput)
	}
	// TODO: a customizer After() to tweak output based on endpoint?
}
