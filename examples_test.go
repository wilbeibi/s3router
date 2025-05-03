// example_test.go
package s3router

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

// ------------------------------------------------------------------
// helpers to spin up dummy S3 endpoints
// ------------------------------------------------------------------

func newCountingServer(code int) (*httptest.Server, *int32) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(code)
		_, _ = io.Copy(io.Discard, r.Body)
	}))
	return srv, &hits
}

func hits(i *int32) int { return int(atomic.LoadInt32(i)) }

// ------------------------------------------------------------------
// YAML template (single string – URLs filled in per test)
// ------------------------------------------------------------------

const yamlTemplate = `
endpoints:
  primary:   %[1]s
  secondary: %[2]s

rules:
  - bucket: photos@primary:test-photos@secondary
    prefix:
      "raw/":
        PutObject:   mirror
        "*":         fallback

      "processed/":
        "*":         primary
`

// ------------------------------------------------------------------
// Tests
// ------------------------------------------------------------------

func TestRouter_CommonPaths(t *testing.T) {
	// ----- mirror ----------------------------------------------------
	t.Run("mirror PutObject", func(t *testing.T) {
		primary, pHits := newCountingServer(200)
		defer primary.Close()
		backup, bHits := newCountingServer(200)
		defer backup.Close()

		cfg := writeTmpYAML(t, primary.URL, backup.URL)
		rt, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		client := &http.Client{Transport: rt}

		req, _ := http.NewRequest(http.MethodPut, "http://dummy/photos/raw/img.jpg", strings.NewReader("test"))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("client.Do: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != 200 || hits(pHits) != 1 || hits(bHits) != 1 {
			t.Fatalf("want 200 and both servers hit once; got code=%d primary=%d backup=%d",
				resp.StatusCode, hits(pHits), hits(bHits))
		}
	})

	// ----- fallback -------------------------------------------------------
	t.Run("fallback on 5xx", func(t *testing.T) {
		primary, pHits := newCountingServer(503) // force failure
		defer primary.Close()
		backup, bHits := newCountingServer(200)
		defer backup.Close()

		cfg := writeTmpYAML(t, primary.URL, backup.URL)
		rt, _ := New(cfg)
		client := &http.Client{Transport: rt}

		req, _ := http.NewRequest(http.MethodGet, "http://dummy/photos/raw/img.jpg", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("client.Do: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != 200 || hits(pHits) != 1 || hits(bHits) != 1 {
			t.Fatalf("want primary 5xx, backup 200; got code=%d primary=%d backup=%d",
				resp.StatusCode, hits(pHits), hits(bHits))
		}
	})

	// ----- ordinary primary ----------------------------------------------
	t.Run("primary route", func(t *testing.T) {
		primary, pHits := newCountingServer(200)
		defer primary.Close()
		backup, bHits := newCountingServer(200)
		defer backup.Close()

		cfg := writeTmpYAML(t, primary.URL, backup.URL)
		rt, _ := New(cfg)
		client := &http.Client{Transport: rt}

		req, _ := http.NewRequest(http.MethodGet, "http://dummy/photos/processed/report.csv", nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("client.Do: %v", err)
		}
		resp.Body.Close()

		if resp.StatusCode != 200 || hits(pHits) != 1 || hits(bHits) != 0 {
			t.Fatalf("want primary 200, backup untouched; got code=%d primary=%d backup=%d",
				resp.StatusCode, hits(pHits), hits(bHits))
		}
	})
}

// ------------------------------------------------------------------
// helper: write YAML to temp file and load via LoadConfig
// ------------------------------------------------------------------

func writeTmpYAML(t *testing.T, primaryURL, backupURL string) *Config {
	t.Helper()
	yml := fmt.Sprintf(yamlTemplate, primaryURL, backupURL)
	tmp := t.TempDir() + "/router.yaml"
	if err := os.WriteFile(tmp, []byte(yml), 0600); err != nil {
		t.Fatalf("writeTmpYAML: %v", err)
	}
	cfg, err := LoadConfig(tmp)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	return cfg
}
