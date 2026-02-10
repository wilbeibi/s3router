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
	"errors"
	"fmt"
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
	// *s3.Client already satisfies store.Store, so pass directly
	return New(cfg, primarySDK, secondarySDK, opts...)
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

func readAllLimited(ctx context.Context, r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, errors.New("maxBytes must be >= 0")
	}
	// +1 so we can detect overflow without allocating unbounded memory.
	lr := &io.LimitedReader{R: r, N: maxBytes + 1}
	data, err := io.ReadAll(lr)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf("body too large to buffer (%d > %d bytes)", len(data), maxBytes)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return data, nil
}

func drainBodyLimited(ctx context.Context, r io.Reader, maxBytes int64) (io.ReadSeeker, io.ReadSeeker, error) {
	data, err := readAllLimited(ctx, r, maxBytes)
	if err != nil {
		return nil, nil, err
	}
	return bytes.NewReader(data), bytes.NewReader(data), nil
}

func teeBody(ctx context.Context, r io.Reader) (io.ReadCloser, io.ReadCloser, error) {
	pr1, pw1 := io.Pipe()
	pr2, pw2 := io.Pipe()
	copyDone := make(chan struct{})
	go func() {
		defer close(copyDone)
		_, err := io.Copy(io.MultiWriter(pw1, pw2), r)
		// Close the writers on completion. If ctx was canceled, prefer its error.
		if cerr := ctx.Err(); cerr != nil {
			_ = pw1.CloseWithError(cerr)
			_ = pw2.CloseWithError(cerr)
			return
		}
		if err != nil {
			_ = pw1.CloseWithError(err)
			_ = pw2.CloseWithError(err)
			return
		}
		_ = pw1.Close()
		_ = pw2.Close()
	}()
	go func() {
		select {
		case <-ctx.Done():
			err := ctx.Err()
			_ = pw1.CloseWithError(err)
			_ = pw2.CloseWithError(err)
		case <-copyDone:
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
