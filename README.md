# s3router

`s3router` is a lightweight `http.RoundTripper` that routes S3-compatible HTTP traffic between two storage endpoints (`primary` & `secondary`) using a simple YAML configuration.

Designed for robust and flexible cloud storage scenarios, it supports:
* **Mirrored writes** for warm-replica durability.
* **Best-effort replicas** without interrupting primary requests.
* **Fallback reads** to secondary storage if the primary endpoint degrades.
* Incremental **bucket migrations** (prefix-by-prefix).

## ✦ Typical Use-Cases

* Create durable backups using mirrored writes.
* Graceful degradation with fallback reads.
* Incremental migrations between storage providers.

## ✦ Installation

```bash
go get github.com/wilbeibi/s3router
```

## ✦ Quick Start Example

```go
cfg, _ := s3router.LoadConfig("router.yaml") // Load & validate configuration
rt,  _ := s3router.New(cfg)                  // Compile router
client := &http.Client{Transport: rt}        // Integrate with standard HTTP clients

// AWS SDK v2 Example
awsCfg, _ := config.LoadDefaultConfig(context.TODO(),
    config.WithHTTPClient(client),
)
s3Client := s3.NewFromConfig(awsCfg)

_, err := s3Client.PutObject(ctx, &s3.PutObjectInput{
    Bucket: aws.String("s3photos"),
    Key:    aws.String("raw/cat.jpg"),
    Body:   bytes.NewReader(img),
})
```

## ✦ Example Configuration (`router.yaml`)

```yaml
endpoints:
  primary: https://s3.us-west-1.amazonaws.com
  secondary: https://r2.cloudflarestorage.com

rules:
  - bucket: s3photos@primary:r2photos@secondary
    prefix:
      "raw/":
        PutObject: mirror         # Both copies must succeed
        DeleteObject: best-effort # Ignore secondary errors
        GetObject: fallback       # Read fallback
        "*": fallback             # Default fallback
      "processed/":
        "*": secondary            # Secondary only
      "*":
        "*": primary              # Default to primary

  - bucket: logs
    prefix:
      "*":
        "*": fallback             # Always fallback
```

## ✦ Routing Keywords Reference

| Keyword       | Behavior                                                                       |
| ------------- | ------------------------------------------------------------------------------ |
| `primary`     | Always primary only.                                                           |
| `secondary`   | Always secondary only.                                                         |
| `mirror`      | Send to both; fail if either copy errors.                                      |
| `best‑effort` | Send to both; return primary result even if secondary errors.                  |
| `fallback`    | Primary; switch to secondary on primary failure (≥400 HTTP or network errors). |

## ✦ Routing Algorithm Explained

1. Parse bucket/key from URL (virtual-host or path-style).
2. Select the rule matching the longest prefix (`photos/raw/` > `photos/*` > `*/*`).
3. Match S3 operation (`PutObject`, `GetObject`, default to `"*"`).
4. Execute the corresponding behavior (`primary`, `secondary`, `mirror`, `best-effort`, `fallback`).

Routing decisions are optimized using O(1) map lookups.

## ✦ Testing

```bash
go test ./...
```

## ✦ Example Customizer

To handle provider-specific quirks, you can implement the `Customizer` interface. We provide a ready-made example for MinIO in `contrib/minio_customizer.go`.

```go
import (
  "net/http"
  "github.com/wilbeibi/s3router/contrib"
)

type MyCustomizer struct{}

func (MyCustomizer) Before(req *http.Request, op, ep string, rule s3router.Rule) {
  // modify req.Header or req.URL here
}

func (MyCustomizer) After(resp *http.Response, op, ep string, rule s3router.Rule) error {
  // inspect or modify resp here
  return nil
}

// primary is MinIO, secondary is another storage need some customizations.
s3router.RegisterCustomizer("primary", contrib.MinioCustomizer{})
s3router.RegisterCustomizer("secondary", MyCustomizer{})
```

## ✦ Roadmap

- [x] Streaming body handling for multi-GB uploads.
- [x] Support external request customizers to adjust request/response behavior for non-standard S3-compatible providers(MinIO, Wasabi, R2...).


## ✦ License

MIT — see [LICENSE](LICENSE).
