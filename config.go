// s3router/config.go
package s3router

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// ---------------- YAML schema ---------------------------------------------

type yamlRule struct {
	Bucket string                       `yaml:"bucket"`
	Prefix map[string]map[string]string `yaml:"prefix"` // prefix → op → action
}

type yamlConfig struct {
	Endpoints map[string]string            `yaml:"endpoints"`
	Rules     []yamlRule                   `yaml:"rules"`
	Routes    map[string]map[string]string `yaml:"routes"`
}

// ---------------- Compiled config -----------------------------------------

type action string

const (
	actPrimary    action = "primary"
	actSecondary  action = "secondary"
	actFallback   action = "fallback"
	actMirror     action = "mirror"
	actBestEffort action = "best-effort"
)

var reserved = map[string]struct{}{
	string(actPrimary): {}, string(actSecondary): {},
	string(actFallback): {}, string(actMirror): {},
	string(actBestEffort): {},
}

type opMap map[string]action   // op → action
type preMap map[string]opMap   // prefix → opMap (longest prefix wins)
type buckMap map[string]preMap // bucket → preMap

type Config struct {
	Endpoints map[string]string
	Rules     buckMap
}

// ---------------- Loader + validation -------------------------------------

func LoadConfig(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var y yamlConfig
	if err := yaml.Unmarshal(raw, &y); err != nil {
		return nil, err
	}

	// support shorthand 'routes' format
	if len(y.Rules) == 0 && len(y.Routes) > 0 {
		// group routes by bucket
		buckets := make(map[string]map[string]map[string]string)
		for route, ops := range y.Routes {
			parts := strings.SplitN(route, "/", 2)
			bucket := parts[0]
			var prefix string
			if len(parts) == 2 {
				prefix = parts[1]
			}
			if buckets[bucket] == nil {
				buckets[bucket] = make(map[string]map[string]string)
			}
			buckets[bucket][prefix] = ops
		}
		// build yamlRule slice
		for bucket, pm := range buckets {
			y.Rules = append(y.Rules, yamlRule{Bucket: bucket, Prefix: pm})
		}
	}

	// ---- sanity checks ----------------------------------------------------
	if _, ok := y.Endpoints["primary"]; !ok {
		return nil, fmt.Errorf("missing mandatory endpoint \"primary\"")
	}

	compiled := make(buckMap)

	for _, r := range y.Rules {
		if r.Bucket == "" {
			return nil, fmt.Errorf("rule with empty bucket")
		}
		if len(r.Prefix) == 0 {
			return nil, fmt.Errorf("bucket %q: no prefix map", r.Bucket)
		}
		prefMap := compiled[r.Bucket]
		if prefMap == nil {
			prefMap = make(preMap)
			compiled[r.Bucket] = prefMap
		}
		for prefix, ops := range r.Prefix {
			if _, dup := prefMap[prefix]; dup {
				return nil, fmt.Errorf("duplicate rule for bucket=%s prefix=%s", r.Bucket, prefix)
			}
			if _, ok := ops["*"]; !ok {
				return nil, fmt.Errorf("bucket=%s prefix=%s: missing \"*\" default", r.Bucket, prefix)
			}
			actionMap := make(opMap)
			for op, tok := range ops {
				act := action(strings.ToLower(tok))
				if _, ok := reserved[string(act)]; !ok {
					return nil, fmt.Errorf("bucket=%s prefix=%s op=%s: unknown action %q",
						r.Bucket, prefix, op, tok)
				}
				actionMap[op] = act
			}
			prefMap[prefix] = actionMap
		}
	}

	return &Config{Endpoints: y.Endpoints, Rules: compiled}, nil
}

// helper: return list of prefixes sorted by length (desc) for fastest match
func (p preMap) sortedPrefixes() []string {
	ps := make([]string, 0, len(p))
	for k := range p {
		ps = append(ps, k)
	}
	sort.Slice(ps, func(i, j int) bool { return len(ps[i]) > len(ps[j]) })
	return ps
}
