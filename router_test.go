package s3router

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
	"github.com/wilbeibi/s3router/store"
)

var (
	primary   = store.NewAWSStore(&s3.Client{})
	secondary = store.NewAWSStore(&s3.Client{})
)

func opString(errOn store.Store, err error) func(context.Context, store.Store, string) (string, error) {
	return func(_ context.Context, st store.Store, _ string) (string, error) {
		if st == errOn {
			return "", err
		}
		if st == primary {
			return "primary", nil
		}
		return "secondary", nil
	}
}

func TestDoSerial_Fallback(t *testing.T) {
	want := "secondary"
	out, err := doSerial(context.Background(),
		opString(primary, io.EOF), // primary fails, secondary succeeds
		"", "", primary, secondary)
	if err != nil {
		t.Fatalf("got unexpected error: %v", err)
	}
	if out != want {
		t.Fatalf("want %q, got %q", want, out)
	}
}

func TestDoParallel_MirrorStrict(t *testing.T) {
	// secondary fails => overall error
	_, err := doParallel(context.Background(), true,
		opString(secondary, io.ErrUnexpectedEOF),
		"", "", primary, secondary)
	if err == nil {
		t.Fatalf("expected error from secondary but got nil")
	}
}

func TestDoParallel_BestEffort(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)

	op := func(_ context.Context, cl store.Store, _ string) (string, error) {
		defer wg.Done()
		if cl == primary {
			return "primary", nil
		}
		return "secondary", nil
	}

	out, err := doParallel(context.Background(), false, /*best‑effort*/
		op, "", "", primary, secondary)
	if err != nil || out != "primary" {
		t.Fatalf("unexpected result: out=%q err=%v", out, err)
	}

	wg.Wait()
}

func TestDispatch_SelectsCorrectClient(t *testing.T) {
	tests := []struct {
		name   string
		act    config.Action
		expect string
	}{
		{"primary", config.ActPrimary, "primary"},
		{"secondary", config.ActSecondary, "secondary"},
		{"fallback", config.ActFallback, "secondary"},
		{"best‑effort", config.ActBestEffort, "primary"},
		{"mirror", config.ActMirror, "primary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			op := opString(nil, nil)
			if tt.act == config.ActFallback {
				// force primary error so fallback path is taken
				op = opString(primary, io.EOF)
			}
			out, err := dispatch(context.Background(), tt.act, op,
				"", "", primary, secondary)
			if err != nil {
				t.Fatalf("dispatch error: %v", err)
			}
			if out != tt.expect {
				t.Fatalf("want %q, got %q", tt.expect, out)
			}
		})
	}
}

func TestDrainBody(t *testing.T) {
	ctx := context.Background()
	want := []byte("hello‑world")
	r1, r2, err := drainBody(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("drainBody error: %v", err)
	}
	b1, _ := io.ReadAll(r1)
	b2, _ := io.ReadAll(r2)
	if !bytes.Equal(b1, want) || !bytes.Equal(b2, want) {
		t.Fatalf("data mismatch; got %q / %q", b1, b2)
	}
}

func TestTeeBody(t *testing.T) {
	ctx := context.Background()
	want := []byte("stream‑content")

	pr1, pr2, err := teeBody(ctx, bytes.NewReader(want))
	if err != nil {
		t.Fatalf("teeBody error: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	var b1, b2 []byte
	go func() { b1, _ = io.ReadAll(pr1); wg.Done() }()
	go func() { b2, _ = io.ReadAll(pr2); wg.Done() }()
	wg.Wait()

	if !bytes.Equal(b1, want) || !bytes.Equal(b2, want) {
		t.Fatalf("data mismatch; got %q / %q", b1, b2)
	}
}
