package contrib

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/wilbeibi/s3router"
)

// startKey is the context key for request start time.
type startKey struct{}

var (
	mu    sync.Mutex
	stats = make(map[string]struct {
		count int
		total time.Duration
	})
)

// MetricsCustomizer implements s3router.Customizer with built-in types.
type MetricsCustomizer struct{}

// Before records current time in the request's context.
func (MetricsCustomizer) Before(req *http.Request, op, ep string, _ s3router.Rule) {
	*req = *req.WithContext(context.WithValue(req.Context(), startKey{}, time.Now()))
}

// After calculates request latency and accumulates metrics.
func (MetricsCustomizer) After(resp *http.Response, op, ep string, _ s3router.Rule) error {
	if resp == nil || resp.Request == nil {
		return nil
	}
	start, ok := resp.Request.Context().Value(startKey{}).(time.Time)
	if !ok {
		return nil
	}
	dur := time.Since(start)
	key := ep + "|" + op

	mu.Lock()
	s := stats[key]
	s.count++
	s.total += dur
	stats[key] = s
	mu.Unlock()
	return nil
}

// Dump prints a simple summary; swap this with Prometheus, OpenTelemetry, etc.
func Dump() {
	mu.Lock()
	defer mu.Unlock()

	fmt.Println("==== S3Router Metrics ====")
	for k, v := range stats {
		parts := strings.SplitN(k, "|", 2)
		avg := time.Duration(0)
		if v.count > 0 {
			avg = v.total / time.Duration(v.count)
		}
		fmt.Printf("endpoint=%q op=%q count=%d avg=%s\n", parts[0], parts[1], v.count, avg)
	}
}
