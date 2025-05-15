# s3router

`s3router` is a lightweight Go library that routes S3-compatible API calls between two storage endpoints (`primary` & `secondary`) using a simple YAML configuration.

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
	f, _ := os.Open("router.yaml")
	defer f.Close()
	routerCfg, _ := config.Load(f)

	primaryClient := s3.NewFromConfig(s3Cfg)
	secondaryClient := s3.NewFromConfig(r2Cfg) // Cloudflare R2 with S3 SDK

	// build the router, wrapping S3 client with the ContentLengthStore wrapper (import "github.com/wilbeibi/s3router/contrib/r2")
	routerClient, _ := s3router.New(routerCfg, primaryClient, secondaryClient)

	_, _ = routerClient.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String("s3photos"),
		Key:    aws.String("raw/cat.jpg"),
		Body:   bytes.NewReader([]byte("hello, world")),
	})
}
```

## ✦ Example Configuration (`router.yaml`)

```yaml
# define the two HTTP endpoints you'll talk to
endpoints:
  primary:   https://s3.us-west-1.amazonaws.com
  secondary: https://r2.cloudflarestorage.com

# map your logical buckets → physical buckets on each endpoint
buckets:
  s3photos:
    primary:   s3photos
    secondary:  r2photos
  logs:
    primary:   logs
    secondary: logs-backup

# routing rules per logical bucket / prefix / operation
rules:
  - bucket: s3photos
    prefix:
      "raw/":
        PutObject: mirror       # both writes must succeed
        DeleteObject: best-effort  # ignore secondary errors
        GetObject: fallback     # fallback‐on‐error reads
        "*": fallback           # default fallback for other ops
      "processed/":
        "*": secondary          # always read/write secondary
      "*":
        "*": primary            # everything else → primary

  - bucket: logs
    prefix:
      "*":
        "*": fallback           # always fallback reads for logs
```

## ✦ Routing Keywords Reference

| Keyword       | Behavior                                                                       |
| ------------- | ------------------------------------------------------------------------------ |
| `primary`     | Always primary only.                                                           |
| `secondary`   | Always secondary only.                                                         |
| `mirror`      | Send to both; fail if either copy errors.                                      |
| `best‑effort` | Send to both; return primary result even if secondary errors.                  |
| `fallback`    | Primary; switch to secondary on primary failure (≥400 HTTP or network errors). |

## ✦ Store Customizer

You can inject custom behaviors into your S3 client. For example, the MyCustomizeClient wrapper auto-sets ContentLength when the body lacks io.Seeker—useful for handling quirks of various S3-compatible providers.

```go
import "github.com/wilbeibi/s3router/contrib/my_customize_client"

myClient := my_customize_client.NewMyCustomizeClient(secondaryClient)
// Wrap the secondary client with the customizer
routerClient, _ := s3router.New(routerCfg, primaryClient, myClient)
```

## ✦ Roadmap

- [x] Streaming body handling for multi-GB uploads.
- [x] Support external request customizers to adjust request/response behavior for non-standard S3-compatible providers(MinIO, Wasabi, R2...).

