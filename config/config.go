package config

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type yamlRule struct {
	Bucket string                       `yaml:"bucket"`
	Prefix map[string]map[string]string `yaml:"prefix"` // prefix → op → action
}

type BucketMapping struct {
	Primary   string `yaml:"primary"`
	Secondary string `yaml:"secondary"`
}

type yamlConfig struct {
	Endpoints map[string]string        `yaml:"endpoints"`
	Buckets   map[string]BucketMapping `yaml:"buckets"`
	Rules     []yamlRule               `yaml:"rules"`
}

// ---------------- Compiled config -----------------------------------------

type Action string // Exported Action type

const (
	ActPrimary    Action = "primary" // Exported constants
	ActSecondary  Action = "secondary"
	ActFallback   Action = "fallback"
	ActMirror     Action = "mirror"
	ActBestEffort Action = "best-effort"
)

// Rule defines a routing rule for a specific bucket/prefix combination.
type Rule struct {
	Bucket  string            `yaml:"bucket"`  // logical bucket name
	Prefix  string            `yaml:"prefix"`  // Prefix within the bucket ("" means root)
	Actions map[string]Action `yaml:"actions"` // op -> action (must contain "*")
}

// Config is the compiled configuration for the S3 router.
type Config struct {
	Endpoints map[string]string        `yaml:"endpoints"`
	Buckets   map[string]BucketMapping `yaml:"buckets"`
	Rules     []Rule                   `yaml:"rules"` // Sorted by Bucket, then longest Prefix first
}

// ---------------- Loader + validation -------------------------------------

// Load reads configuration from the given reader and returns a compiled Config.
func Load(r io.Reader) (*Config, error) {
	var yml yamlConfig
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(&yml); err != nil {
		return nil, err
	}

	// Transform yamlConfig into Config
	cfg := &Config{
		Endpoints: yml.Endpoints,
		Buckets:   yml.Buckets,
		Rules:     make([]Rule, 0, len(yml.Rules)),
	}

	// Transform each yamlRule into multiple Rules (one per prefix)
	for _, yr := range yml.Rules {
		for prefix, actions := range yr.Prefix {
			// Convert "*" prefix to empty string for root
			rulePrefix := prefix
			if prefix == "*" {
				rulePrefix = ""
			}

			rule := Rule{
				Bucket:  yr.Bucket,
				Prefix:  rulePrefix,
				Actions: make(map[string]Action, len(actions)),
			}
			for op, action := range actions {
				rule.Actions[op] = Action(action)
			}
			cfg.Rules = append(cfg.Rules, rule)
		}
	}

	// Validate default operation
	for _, rule := range cfg.Rules {
		if _, ok := rule.Actions["*"]; !ok {
			return nil, fmt.Errorf("missing default \"*\" operation")
		}
	}

	// Sort rules by bucket and then by prefix descending (lexicographically)
	sort.Slice(cfg.Rules, func(i, j int) bool {
		// First sort by bucket
		if cfg.Rules[i].Bucket != cfg.Rules[j].Bucket {
			return cfg.Rules[i].Bucket < cfg.Rules[j].Bucket
		}
		// Then by prefix lex descending
		return cfg.Rules[i].Prefix > cfg.Rules[j].Prefix
	})

	return cfg, nil
}

// Lookup finds the best matching rule and action for a given bucket, key, and operation.
// If no matching rule is found, defaults to primary.
func (cfg *Config) Lookup(bucket, key, op string) (Rule, Action) {
	fmt.Printf("[debug] Looking up rule for bucket=%s, key=%s, op=%s\n", bucket, key, op)
	for _, rule := range cfg.Rules {
		fmt.Printf("[debug] Checking rule: bucket=%s, prefix=%s, actions=%v\n", rule.Bucket, rule.Prefix, rule.Actions)
		if rule.Bucket != bucket && rule.Bucket != "*" {
			fmt.Printf("[debug] Bucket mismatch, skipping rule\n")
			continue
		}
		if rule.Prefix != "" && !strings.HasPrefix(key, rule.Prefix) {
			fmt.Printf("[debug] Prefix mismatch, skipping rule\n")
			continue
		}
		if act, ok := rule.Actions[op]; ok {
			fmt.Printf("[debug] Found exact operation match: %s\n", act)
			return rule, act
		}
		fmt.Printf("[debug] Using wildcard operation: %s\n", rule.Actions["*"])
		return rule, rule.Actions["*"]
	}
	fmt.Printf("[debug] No matching rule found, defaulting to primary\n")
	return Rule{}, ActPrimary
}

// GetPhysicalBucket returns the physical bucket name for the given logical bucket and endpoint.
// If no mapping is found, returns the input bucket name.
func (cfg *Config) GetPhysicalBucket(logicalBucket, endpoint string) string {
	if mapping, ok := cfg.Buckets[logicalBucket]; ok {
		switch endpoint {
		case "primary":
			return mapping.Primary
		case "secondary":
			return mapping.Secondary
		}
	}
	return logicalBucket
}

// GetLogicalBucket returns the logical bucket name for the given physical bucket name.
// If no mapping is found, returns the input bucket name.
func (cfg *Config) GetLogicalBucket(physicalBucket string) string {
	for name, mapping := range cfg.Buckets {
		if mapping.Primary == physicalBucket || mapping.Secondary == physicalBucket {
			return name
		}
	}
	return physicalBucket
}

// IsLogicalBucket returns true if the given bucket name is a logical bucket defined in the configuration.
func (cfg *Config) IsLogicalBucket(bucket string) bool {
	_, ok := cfg.Buckets[bucket]
	return ok
}
