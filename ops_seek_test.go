package s3router

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/wilbeibi/s3router/config"
)

type stubStore struct{}

func (s stubStore) GetObject(ctx context.Context, in *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) HeadObject(ctx context.Context, in *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) DeleteObject(ctx context.Context, in *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) DeleteObjects(ctx context.Context, in *s3.DeleteObjectsInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectsOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) ListObjectsV2(ctx context.Context, in *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) UploadPart(ctx context.Context, in *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) ListParts(ctx context.Context, in *s3.ListPartsInput, optFns ...func(*s3.Options)) (*s3.ListPartsOutput, error) {
	return nil, errors.New("not implemented")
}
func (s stubStore) AbortMultipartUpload(ctx context.Context, in *s3.AbortMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.AbortMultipartUploadOutput, error) {
	return nil, errors.New("not implemented")
}

type recordStore struct {
	stubStore
	putErr           error
	uploadPartErr    error
	putBodies        [][]byte
	uploadPartBodies [][]byte
}

func (s *recordStore) PutObject(ctx context.Context, in *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	b, _ := io.ReadAll(in.Body)
	s.putBodies = append(s.putBodies, b)
	if s.putErr != nil {
		return nil, s.putErr
	}
	return &s3.PutObjectOutput{}, nil
}

func (s *recordStore) UploadPart(ctx context.Context, in *s3.UploadPartInput, _ ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	b, _ := io.ReadAll(in.Body)
	s.uploadPartBodies = append(s.uploadPartBodies, b)
	if s.uploadPartErr != nil {
		return nil, s.uploadPartErr
	}
	return &s3.UploadPartOutput{}, nil
}

func testCfg(action config.Action) *config.Config {
	return &config.Config{
		Endpoints: map[config.Endpoint]string{
			config.EndpointPrimary:   "http://primary",
			config.EndpointSecondary: "http://secondary",
		},
		Buckets: map[string]config.BucketMapping{
			"lb": {Primary: "pb1", Secondary: "pb2"},
		},
		Rules: []config.Rule{
			{
				Bucket: "lb",
				Prefix: "",
				Actions: map[string]config.Action{
					"*":          action,
					"PutObject":  action,
					"UploadPart": action,
				},
			},
		},
	}
}

type readOnce struct{ r io.Reader }

func (r *readOnce) Read(p []byte) (int, error) { return r.r.Read(p) }

func TestPutObject_BestEffort_RequiresReadSeeker(t *testing.T) {
	primary := &recordStore{}
	secondary := &recordStore{}
	r, err := New(testCfg(config.ActBestEffort), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.PutObjectInput{
		Bucket: aws.String("lb"),
		Key:    aws.String("k"),
		Body:   &readOnce{r: bytes.NewReader([]byte("x"))},
	}
	if _, err := r.PutObject(context.Background(), in); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestPutObject_BestEffort_PrimarySuccessAlsoCallsSecondary(t *testing.T) {
	primary := &recordStore{}
	secondary := &recordStore{}
	r, err := New(testCfg(config.ActBestEffort), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	body := bytes.NewReader([]byte("hello"))
	if _, err := body.Seek(2, io.SeekStart); err != nil {
		t.Fatalf("seek error: %v", err)
	}

	in := &s3.PutObjectInput{
		Bucket: aws.String("lb"),
		Key:    aws.String("k"),
		Body:   body,
	}
	if _, err := r.PutObject(context.Background(), in); err != nil {
		t.Fatalf("PutObject error: %v", err)
	}

	if len(primary.putBodies) != 1 || len(secondary.putBodies) != 1 {
		t.Fatalf("expected 1 primary and 1 secondary calls, got %d/%d", len(primary.putBodies), len(secondary.putBodies))
	}
	if string(primary.putBodies[0]) != "llo" || string(secondary.putBodies[0]) != "llo" {
		t.Fatalf("body mismatch: primary=%q secondary=%q", primary.putBodies[0], secondary.putBodies[0])
	}
}

func TestPutObject_BestEffort_PrimarySuccessIgnoresSecondaryError(t *testing.T) {
	primary := &recordStore{}
	secondary := &recordStore{putErr: errors.New("secondary failed")}
	r, err := New(testCfg(config.ActBestEffort), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.PutObjectInput{
		Bucket: aws.String("lb"),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("hello")),
	}
	if _, err := r.PutObject(context.Background(), in); err != nil {
		t.Fatalf("expected primary success to be returned, got error: %v", err)
	}
	if len(secondary.putBodies) != 1 || string(secondary.putBodies[0]) != "hello" {
		t.Fatalf("expected secondary attempt with full body, got %q", secondary.putBodies)
	}
}

func TestPutObject_BestEffort_FallsBackOnPrimaryError(t *testing.T) {
	primary := &recordStore{putErr: errors.New("primary failed")}
	secondary := &recordStore{}
	r, err := New(testCfg(config.ActBestEffort), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.PutObjectInput{
		Bucket: aws.String("lb"),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("hello")),
	}
	if _, err := r.PutObject(context.Background(), in); err != nil {
		t.Fatalf("expected best-effort fallback to succeed, got error: %v", err)
	}
	if len(secondary.putBodies) != 1 || string(secondary.putBodies[0]) != "hello" {
		t.Fatalf("expected secondary to receive body, got %q", secondary.putBodies)
	}
}

func TestPutObject_Fallback_ReplaysReadSeekerOnPrimaryError(t *testing.T) {
	primary := &recordStore{putErr: errors.New("primary failed")}
	secondary := &recordStore{}
	r, err := New(testCfg(config.ActFallback), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.PutObjectInput{
		Bucket: aws.String("lb"),
		Key:    aws.String("k"),
		Body:   bytes.NewReader([]byte("hello")),
	}
	if _, err := r.PutObject(context.Background(), in); err != nil {
		t.Fatalf("expected fallback to succeed, got error: %v", err)
	}
	if len(secondary.putBodies) != 1 || string(secondary.putBodies[0]) != "hello" {
		t.Fatalf("expected secondary to receive body, got %q", secondary.putBodies)
	}
}

func TestUploadPart_BestEffort_RequiresReadSeeker(t *testing.T) {
	primary := &recordStore{}
	secondary := &recordStore{}
	r, err := New(testCfg(config.ActBestEffort), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.UploadPartInput{
		Bucket:     aws.String("lb"),
		Key:        aws.String("k"),
		PartNumber: aws.Int32(1),
		UploadId:   aws.String("u"),
		Body:       &readOnce{r: bytes.NewReader([]byte("x"))},
	}
	if _, err := r.UploadPart(context.Background(), in); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestUploadPart_BestEffort_FallsBackOnPrimaryError(t *testing.T) {
	primary := &recordStore{uploadPartErr: errors.New("primary failed")}
	secondary := &recordStore{}
	r, err := New(testCfg(config.ActBestEffort), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.UploadPartInput{
		Bucket:     aws.String("lb"),
		Key:        aws.String("k"),
		PartNumber: aws.Int32(1),
		UploadId:   aws.String("u"),
		Body:       bytes.NewReader([]byte("part")),
	}
	if _, err := r.UploadPart(context.Background(), in); err != nil {
		t.Fatalf("expected best-effort fallback to succeed, got error: %v", err)
	}
	if len(secondary.uploadPartBodies) != 1 || string(secondary.uploadPartBodies[0]) != "part" {
		t.Fatalf("expected secondary to receive body, got %q", secondary.uploadPartBodies)
	}
}

func TestUploadPart_BestEffort_PrimarySuccessIgnoresSecondaryError(t *testing.T) {
	primary := &recordStore{}
	secondary := &recordStore{uploadPartErr: errors.New("secondary failed")}
	r, err := New(testCfg(config.ActBestEffort), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.UploadPartInput{
		Bucket:     aws.String("lb"),
		Key:        aws.String("k"),
		PartNumber: aws.Int32(1),
		UploadId:   aws.String("u"),
		Body:       bytes.NewReader([]byte("part")),
	}
	if _, err := r.UploadPart(context.Background(), in); err != nil {
		t.Fatalf("expected primary success to be returned, got error: %v", err)
	}
	if len(secondary.uploadPartBodies) != 1 || string(secondary.uploadPartBodies[0]) != "part" {
		t.Fatalf("expected secondary attempt with full body, got %q", secondary.uploadPartBodies)
	}
}

func TestUploadPart_Fallback_ReplaysReadSeekerOnPrimaryError(t *testing.T) {
	primary := &recordStore{uploadPartErr: errors.New("primary failed")}
	secondary := &recordStore{}
	r, err := New(testCfg(config.ActFallback), primary, secondary)
	if err != nil {
		t.Fatalf("New error: %v", err)
	}

	in := &s3.UploadPartInput{
		Bucket:     aws.String("lb"),
		Key:        aws.String("k"),
		PartNumber: aws.Int32(1),
		UploadId:   aws.String("u"),
		Body:       bytes.NewReader([]byte("part")),
	}
	if _, err := r.UploadPart(context.Background(), in); err != nil {
		t.Fatalf("expected fallback to succeed, got error: %v", err)
	}
	if len(secondary.uploadPartBodies) != 1 || string(secondary.uploadPartBodies[0]) != "part" {
		t.Fatalf("expected secondary to receive body, got %q", secondary.uploadPartBodies)
	}
}
