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
	"github.com/wilbeibi/s3router/store"
)

// Option configures the router.
type Option func(*router)

// WithMaxBufferBytes sets the streaming buffer size.
func WithMaxBufferBytes(n int64) Option {
	return func(c *router) {
		c.maxBufferBytes = n
	}
}

// New builds the facade around two pre-configured stores.
func New(cfg *config.Config,
	primary, secondary store.Store,
	opts ...Option) (store.Store, error) {
	c := &router{
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

// S3Clients is a convenience function that wraps s3.Clients in AWSStore
// before calling New.
func S3Clients(cfg *config.Config,
	primarySDK, secondarySDK *s3.Client,
	opts ...Option) (store.Store, error) {
	primaryStore := store.NewAWSStore(primarySDK)
	secondaryStore := store.NewAWSStore(secondarySDK)
	return New(cfg, primaryStore, secondaryStore, opts...)
}

type router struct {
	cfg            *config.Config
	primary        store.Store
	secondary      store.Store
	maxBufferBytes int64 // 256 MiB default
}

// Serial "primary-then-secondary if needed" (fallback).
func doSerial[I any, T any](
	ctx context.Context,
	op func(context.Context, store.Store, I) (T, error),
	in1, in2 I,
	s1, s2 store.Store,
) (T, error) {
	out, err := op(ctx, s1, in1)
	if err == nil {
		return out, nil
	}
	return op(ctx, s2, in2)
}

// Parallel dual-write/read. strict==true => mirror; false => best-effort.
func doParallel[I any, T any](
	ctx context.Context,
	strict bool,
	op func(context.Context, store.Store, I) (T, error),
	in1, in2 I,
	s1, s2 store.Store,
) (T, error) {
	if strict {
		var wg sync.WaitGroup
		var out T
		var errA, errB error
		wg.Add(2)
		go func() {
			defer wg.Done()
			out, errA = op(ctx, s1, in1)
		}()
		go func() {
			defer wg.Done()
			_, errB = op(ctx, s2, in2)
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
	out, err := op(ctx, s1, in1)
	go func() {
		_, _ = op(ctx, s2, in2)
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
	op func(context.Context, store.Store, I) (T, error),
	primaryInput, secondaryInput I,
	s1, s2 store.Store,
) (T, error) {
	switch action {
	case config.ActPrimary:
		return op(ctx, s1, primaryInput)
	case config.ActSecondary:
		return op(ctx, s2, secondaryInput)
	case config.ActFallback:
		return doSerial(ctx, op, primaryInput, secondaryInput, s1, s2)
	case config.ActBestEffort:
		return doParallel(ctx, false, op, primaryInput, secondaryInput, s1, s2)
	case config.ActMirror:
		return doParallel(ctx, true, op, primaryInput, secondaryInput, s1, s2)
	default:
		// Fall back to primary if action is unknown
		return op(ctx, s1, primaryInput)
	}
}
