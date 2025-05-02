# s3router

`s3router` is a lightweight `http.RoundTripper` that **routes S3‑compatible HTTP traffic** between exactly two storage endpoints—`primary` and `secondary`—using a declarative YAML file.

Typical use‑cases ⬇︎
* **Mirrored writes** for warm‑replica durability.
* **Best‑effort replicas** that never break the primary path.
* **Read fallback** when the primary region degrades.
* Incremental **bucket migration** (prefix‑by‑prefix).

It works under the AWS Go SDK v1 **and** v2, or any plain `http.Client`‑based code.

```
+-----------------------+
| AWS SDK / restic ...  |
| (http.Client)         |
+-----------------------+
│ PutObject photos/raw/*
▼
+-----------------------+ → mirror      → primary + secondary
| s3router              | → fallback    → primary │ secondary
+-----------------------+ → best-effort → primary + secondary*
│                         → primary     → primary
▼
+-----------------------+
| net/http Transport    |
+-----------------------+
*secondary errors ignored
```

## ✦ Install

```bash
go get github.com/wilbeibi/s3router
```

## ✦ Quick start

```go
cfg, _ := s3router.LoadConfig("router.yaml") // 1) parse & validate
rt,  _ := s3router.New(cfg)                  // 2) compile router
client := &http.Client{Transport: rt}        // 3) use everywhere

// AWS SDK v2 setup with custom transport
awsCfg, _ := config.LoadDefaultConfig(context.TODO(),
    config.WithHTTPClient(client),
)
s3Client := s3.NewFromConfig(awsCfg) 

// This PutObject now follows your routing rules.
*, * = s3.PutObject(ctx, &s3.PutObjectInput{
    Bucket: aws.String("photos"),
    Key:    aws.String("raw/cat.jpg"),
    Body:   bytes.NewReader(img),
})
```

## ✦ Configuration file (router.yaml)

```yaml
endpoints:
  primary:   https://s3.us‑west‑1.amazonaws.com
  secondary: https://r2.cloudflarestorage.com

rules:
  - bucket: photos
    prefix:
      "raw/":
        PutObject:    mirror       # both copies must succeed
        DeleteObject: best-effort  # ignore replica errors
        GetObject:    fallback     # read fallback
        "*":          fallback     # default for this prefix
      "processed/":
        "*":         secondary     # always secondary
      "*":                         # catch‑all inside bucket
        "*":         primary
  - bucket: logs
    prefix:
      "*":
        "*":         fallback
```

## 5 keywords

| Keyword     | Behavior                                                         |
| ----------- | ---------------------------------------------------------------- |
| primary     | Always primary only.                                             |
| secondary   | Always secondary only.                                           |
| fallback    | Primary; if network error or HTTP ≥ 500 → secondary.             |
| best‑effort | PUT/DELETE on both; return primary result even on replica error. |
| mirror      | PUT/DELETE on both; fail if either copy errors.                  |

## Validation rules

- primary endpoint must exist.
- If any rule uses mirror, best‑effort, or fallback, secondary must exist.
- Each (bucket,prefix) appears once and contains a "*": … default.
- Unknown keywords abort startup.

## ✦ Routing algorithm

1. Parse bucket + key from the URL (virtual‑host or path style).
2. Pick the rule whose prefix is the longest match (photos/raw/ > photos/* > */*).
3. Inside that block choose the S3 operation (PutObject, ListObjectsV2, …); default to "*" if absent.
4. Execute one of the 5 keyword behaviours.

All look‑ups are O(1) map accesses.

## ✦ Large uploads

The default helper buffers the whole body to replay it to the replica. Swap in a streaming version (io.TeeReader → temp file) for multi‑GB PUTs.

## ✦ Hot reload (optional)

```go
var rtab atomic.Value // holds *http.RoundTripper
go func() {
    w, _ := fsnotify.NewWatcher()
    w.Add("router.yaml")
    for range w.Events {
        cfg, err := s3router.LoadConfig("router.yaml")
        if err != nil { log.Printf("bad config: %v", err); continue }
        rt, _ := s3router.New(cfg)
        rtab.Store(rt)                       // atomic swap
    }
}()
```

## ✦ Tests

Run the built‑in suite (uses httptest.Server):

```bash
go test ./...
```

Covers mirror, best‑effort, fallback, and primary scenarios.

## ✦ Roadmap

- Prometheus metrics hook.
- Per‑rule time‑out knobs.
- Optional third endpoint ("tertiary") support.

## ✦ License

MIT — see LICENSE.
