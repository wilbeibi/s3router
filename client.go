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

package router

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
)

type Client interface { // facade exposed to user code
	GetObject(ctx context.Context, in *s3.GetObjectInput,
		optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, in *s3.PutObjectInput,
		optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	// …more ops you care about
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

func (c *client) GetObject(
	ctx context.Context,
	in *s3.GetObjectInput,
	optFns ...func(*s3.Options),
) (*s3.GetObjectOutput, error) {
	const op = "GetObject"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	_, action := c.cfg.Lookup(bucket, key, op)
	return dispatch(ctx, action,
		func(ctx context.Context, cl *s3.Client) (*s3.GetObjectOutput, error) {
			return cl.GetObject(ctx, in, optFns...)
		},
		func(ctx context.Context, cl *s3.Client) (*s3.GetObjectOutput, error) {
			return cl.GetObject(ctx, in, optFns...)
		}, c.primary, c.secondary)
}

// Serial "primary-then-secondary if needed" (fallback).
func doSerial[T any](
	ctx context.Context,
	first, second func(context.Context, *s3.Client) (T, error),
	c1, c2 *s3.Client,
) (T, error) {
	out, err := first(ctx, c1)
	if err == nil {
		return out, nil
	}
	return second(ctx, c2)
}

// Parallel dual-write/read. strict==true => mirror; false => best-effort.
func doParallel[T any](
	ctx context.Context,
	strict bool,
	a, b func(context.Context, *s3.Client) (T, error),
	c1, c2 *s3.Client,
) (T, error) {
	if strict {
		var wg sync.WaitGroup
		var out T
		var errA, errB error
		wg.Add(2)
		go func() {
			defer wg.Done()
			out, errA = a(ctx, c1)
		}()
		go func() {
			defer wg.Done()
			_, errB = b(ctx, c2)
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
	out, err := a(ctx, c1)
	go func() {
		_, _ = b(ctx, c2)
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

func (c *client) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	const op = "PutObject"
	bucket, key := aws.ToString(in.Bucket), aws.ToString(in.Key)
	_, action := c.cfg.Lookup(bucket, key, op)
	fmt.Println("action", action)

	var primaryFn, secondaryFn func(context.Context, *s3.Client) (*s3.PutObjectOutput, error)

	if action == config.ActMirror && in.Body != nil {
		in1 := *in
		in2 := *in
		var r1, r2 io.Reader
		var err error
		if in.ContentLength != nil && *in.ContentLength < c.maxBufferBytes {
			r1, r2, err = drainBody(ctx, in.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to drain request body: %w", err)
			}
		} else {
			r1, r2, err = teeBody(ctx, in.Body)
			if err != nil {
				return nil, fmt.Errorf("failed to split request body: %w", err)
			}
		}
		in1.Body = r1
		in2.Body = r2

		primaryFn = func(ctx context.Context, cl *s3.Client) (*s3.PutObjectOutput, error) {
			return cl.PutObject(ctx, &in1, optFns...)
		}
		secondaryFn = func(ctx context.Context, cl *s3.Client) (*s3.PutObjectOutput, error) {
			return cl.PutObject(ctx, &in2, optFns...)
		}
	} else {
		primaryFn = func(ctx context.Context, cl *s3.Client) (*s3.PutObjectOutput, error) {
			return cl.PutObject(ctx, in, optFns...)
		}
		secondaryFn = primaryFn
	}

	return dispatch(ctx, action, primaryFn, secondaryFn, c.primary, c.secondary)
}

// dispatch executes the primary and secondary functions according to action.
func dispatch[T any](
	ctx context.Context,
	action config.Action,
	primaryFn, secondaryFn func(context.Context, *s3.Client) (T, error),
	c1, c2 *s3.Client,
) (T, error) {
	switch action {
	case config.ActPrimary:
		return primaryFn(ctx, c1)
	case config.ActSecondary:
		return secondaryFn(ctx, c2)
	case config.ActFallback:
		return doSerial(ctx, primaryFn, secondaryFn, c1, c2)
	case config.ActBestEffort:
		return doParallel(ctx, false, primaryFn, secondaryFn, c1, c2)
	case config.ActMirror:
		return doParallel(ctx, true, primaryFn, secondaryFn, c1, c2)
	default:
		// Fall back to primary if action is unknown
		return primaryFn(ctx, c1)
	}
}
