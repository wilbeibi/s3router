package s3router

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
			name: "complex rules",
			yaml: `
endpoints:
  primary: http://primary:9000
  secondary: http://secondary:9000
  fallback: http://fallback:8000
rules:
  - bucket: main-data@primary:data-replica@secondary
    prefix:
      "": { "*": mirror }
      images/: { "*": primary }
      images/large/: { GetObject: secondary, "*": primary }
  - bucket: bucket-logs
    prefix:
      processed/: { "*": fallback }
      "": { "*": secondary }
`,
			want: &Config{
				Endpoints: map[string]string{
					"primary":   "http://primary:9000",
					"secondary": "http://secondary:9000",
					"fallback":  "http://fallback:8000",
				},
				Rules: []Rule{
					{
						Bucket:    "bucket-logs",
						Alias:     map[string]string{"primary": "bucket-logs"},
						Prefix:    "processed/",
						ActionFor: map[string]action{"*": actFallback},
					},
					{
						Bucket:    "bucket-logs",
						Alias:     map[string]string{"primary": "bucket-logs"},
						Prefix:    "",
						ActionFor: map[string]action{"*": actSecondary},
					},
					{
						Bucket:    "main-data",
						Alias:     map[string]string{"primary": "main-data", "secondary": "data-replica"},
						Prefix:    "images/large/",
						ActionFor: map[string]action{"GetObject": actSecondary, "*": actPrimary},
					},
					{
						Bucket:    "main-data",
						Alias:     map[string]string{"primary": "main-data", "secondary": "data-replica"},
						Prefix:    "images/",
						ActionFor: map[string]action{"*": actPrimary},
					},
					{
						Bucket:    "main-data",
						Alias:     map[string]string{"primary": "main-data", "secondary": "data-replica"},
						Prefix:    "",
						ActionFor: map[string]action{"*": actMirror},
					},
				},
			},
		},
		{
			name: "missing default operation",
			yaml: `
endpoints:
  primary: http://p:9000
rules:
  - bucket: test@primary
    prefix:
      data/:
        GetObject: primary
`,
			wantErr: "missing default \"*\" operation",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := LoadConfig(strings.NewReader(tc.yaml))

			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("LoadConfig() error = %v, want error containing %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("LoadConfig() = %+v, want %+v", got, tc.want)
			}
		})
	}
}
