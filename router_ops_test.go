package s3router

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
)

type mockBodyStore struct {
	name string
	t    *testing.T
	// Optional: Simulate error for primary/secondary
	fail bool
	// Optional: WaitGroup to signal completion
	wg *sync.WaitGroup
}

func (m *mockBodyStore) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	if m.wg != nil {
		defer m.wg.Done()
	}
	if in.Body == nil {
		m.t.Errorf("[%s] PutObject body is nil", m.name)
		return nil, nil
	}
	// Simulate reading the body
	data, err := io.ReadAll(in.Body)
	if err != nil {
		m.t.Errorf("[%s] failed to read body: %v", m.name, err)
		return nil, err
	}

	if m.fail {
		return nil, io.EOF // Simulate error
	}

	expected := "hello world"
	if string(data) != expected {
		m.t.Errorf("[%s] got body %q, want %q", m.name, string(data), expected)
	} else {
		// m.t.Logf("[%s] successfully read body", m.name)
	}
	return &s3.PutObjectOutput{}, nil
}
func (m *mockBodyStore) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return nil, nil
}
func (m *mockBodyStore) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) UploadPart(ctx context.Context, in *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) ListParts(ctx context.Context, in *s3.ListPartsInput, optFns ...func(*s3.Options)) (*s3.ListPartsOutput, error) {
	return nil, nil
}
func (m *mockBodyStore) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, nil
}

func stringPtr(s string) *string {
	return &s
}

func TestPutObject_BestEffort_Concurrent(t *testing.T) {
	cfgYaml := `
endpoints:
  primary: http://p
  secondary: http://s
buckets:
  testbucket:
    primary: p
    secondary: s
rules:
  - bucket: testbucket
    prefix:
      "*":
        PutObject: best-effort
        "*": primary
`
	cfg, err := config.Load(strings.NewReader(cfgYaml))
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)

	primary := &mockBodyStore{name: "primary", t: t, wg: &wg}
	secondary := &mockBodyStore{name: "secondary", t: t, wg: &wg}

	r, err := New(cfg, primary, secondary)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	// Use a pipe to simulate a stream
	pr, pw := io.Pipe()
	go func() {
		pw.Write([]byte("hello world"))
		pw.Close()
	}()

	ctx := context.Background()
	_, err = r.PutObject(ctx, &s3.PutObjectInput{
		Bucket: stringPtr("testbucket"),
		Key:    stringPtr("obj"),
		Body:   pr,
	})
	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}

	// Wait for both primary (which finishes before PutObject returns)
	// and secondary (which finishes async) to complete.
	wg.Wait()
}

func TestPutObject_Fallback_SmallBody(t *testing.T) {
	// Fallback should work for small bodies by buffering
	cfgYaml := `
endpoints:
  primary: http://p
  secondary: http://s
buckets:
  testbucket:
    primary: p
    secondary: s
rules:
  - bucket: testbucket
    prefix:
      "*":
        PutObject: fallback
        "*": primary
`
	cfg, err := config.Load(strings.NewReader(cfgYaml))
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}

	primary := &mockBodyStore{name: "primary", t: t, fail: true} // Primary fails
	secondary := &mockBodyStore{name: "secondary", t: t}

	r, err := New(cfg, primary, secondary)
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}

	bodyContent := "hello world"
	contentLength := int64(len(bodyContent))
	// ContentLength is optional but drainBody uses it to decide buffering if available.
	// If nil, it checks maxBufferBytes which is large by default.
	// So drainBody will buffer "hello world" only if ContentLength is set and small.

	ctx := context.Background()
	_, err = r.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        stringPtr("testbucket"),
		Key:           stringPtr("obj"),
		Body:          strings.NewReader(bodyContent),
		ContentLength: &contentLength,
	})

	// Even if it is strings.NewReader (seekable), drainBody reads it all.
	// Primary mock reads it all (fails).
	// Secondary mock should receive a fresh reader with same content.

	if err != nil {
		t.Fatalf("PutObject failed: %v", err)
	}
}
