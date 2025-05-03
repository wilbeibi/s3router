package s3router

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

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

// Rule defines a routing rule for a specific bucket/prefix combination.
type Rule struct {
	Bucket    string            // Canonical (primary) bucket name
	Alias     map[string]string // Optional: endpoint -> bucket name override
	Prefix    string            // Prefix within the bucket ("" means root)
	ActionFor map[string]action // op -> action (must contain "*")
}

type Config struct {
	Endpoints map[string]string
	Rules     []Rule // Sorted by Bucket, then longest Prefix first
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

	// support shorthand 'routes' format (convert to rules)
	if len(y.Rules) == 0 && len(y.Routes) > 0 {
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
		y.Rules = nil // Clear routes now that they are converted
		for bucket, pm := range buckets {
			y.Rules = append(y.Rules, yamlRule{Bucket: bucket, Prefix: pm})
		}
	}

	if _, ok := y.Endpoints["primary"]; !ok {
		return nil, fmt.Errorf("missing mandatory endpoint \"primary\"")
	}

	var rules []Rule
	for _, yr := range y.Rules {
		// Parse bucket string (e.g., "bucket@primary:alias@secondary")
		aliasMap := map[string]string{}
		canonBucket := ""
		for _, tok := range strings.Split(yr.Bucket, ":") {
			parts := strings.SplitN(tok, "@", 2)
			name, ep := parts[0], "primary"
			if len(parts) == 2 {
				ep = parts[1]
			}
			if name == "" {
				return nil, fmt.Errorf("empty bucket name in rule: %q", yr.Bucket)
			}
			if ep == "primary" {
				canonBucket = name
			}
			aliasMap[ep] = name
		}
		if canonBucket == "" {
			return nil, fmt.Errorf("rule missing primary bucket definition (@primary): %q", yr.Bucket)
		}

		if len(yr.Prefix) == 0 {
			return nil, fmt.Errorf("bucket %q: rule block is empty", yr.Bucket)
		}

		// Create a Rule for each prefix
		for prefix, ops := range yr.Prefix {
			if len(ops) == 0 {
				return nil, fmt.Errorf("bucket %q prefix %q: no operations defined", yr.Bucket, prefix)
			}
			if _, ok := ops["*"]; !ok {
				return nil, fmt.Errorf("bucket %q prefix %q: missing default \"*\" operation", yr.Bucket, prefix)
			}

			actionMap := make(map[string]action)
			for op, tok := range ops {
				act := action(strings.ToLower(tok))
				if _, ok := reserved[string(act)]; !ok {
					return nil, fmt.Errorf("bucket %q prefix %q op %q: unknown action %q",
						yr.Bucket, prefix, op, tok)
				}
				actionMap[op] = act
			}

			// Create the final Rule object
			rule := Rule{
				Bucket:    canonBucket,
				Alias:     aliasMap,
				Prefix:    prefix, // Use the specific prefix from the map key
				ActionFor: actionMap,
			}
			rules = append(rules, rule)
		}
	}

	// Sort rules: by canonical bucket, then longest prefix first
	sort.Slice(rules, func(i, j int) bool {
		if rules[i].Bucket != rules[j].Bucket {
			return rules[i].Bucket < rules[j].Bucket
		}
		// Secondary sort: longest prefix wins
		return len(rules[i].Prefix) > len(rules[j].Prefix)
	})

	return &Config{Endpoints: y.Endpoints, Rules: rules}, nil
}
