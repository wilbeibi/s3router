package config

import (
	"reflect"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		want    *Config
		wantErr string
	}{
		{
			name: "single bucket with different prefixes",
			yaml: `
endpoints:
  primary: http://primary:9000
  secondary: http://secondary:9000

buckets:
  photos:
    primary: photos
    secondary: cf-photos

rules:
  - bucket: photos
    prefix:
      "raw/":
        PutObject: mirror # primary & secondary must succeed
        DeleteObject: best-effort # if primary fails, return secondary's response
        CompleteMultipartUpload: mirror
        GetObject: fallback
        "*": fallback # all other ops → fallback read path
      "processed/":
        "*": secondary # only send to secondary
      "*": # remaining prefixes
        "*": primary # only send to primary
`,
			want: &Config{
				Endpoints: map[string]string{
					"primary":   "http://primary:9000",
					"secondary": "http://secondary:9000",
				},
				Buckets: map[string]BucketMapping{
					"photos": {
						Primary:   "photos",
						Secondary: "cf-photos",
					},
				},
				Rules: []Rule{
					{
						Bucket: "photos",
						Prefix: "raw/",
						Actions: map[string]Action{
							"PutObject":               ActMirror,
							"DeleteObject":            ActBestEffort,
							"CompleteMultipartUpload": ActMirror,
							"GetObject":               ActFallback,
							"*":                       ActFallback,
						},
					},
					{
						Bucket: "photos",
						Prefix: "processed/",
						Actions: map[string]Action{
							"*": ActSecondary,
						},
					},
					{
						Bucket: "photos",
						Prefix: "",
						Actions: map[string]Action{
							"*": ActPrimary,
						},
					},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Load(strings.NewReader(tc.yaml))

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load() error = %v, want error containing %q", err, tc.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Load() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
