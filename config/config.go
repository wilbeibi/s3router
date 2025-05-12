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

type Action string
type Endpoint string

const (
	ActPrimary    Action = "primary"
	ActSecondary  Action = "secondary"
	ActFallback   Action = "fallback"
	ActMirror     Action = "mirror"
	ActBestEffort Action = "best-effort"

	EndpointPrimary   Endpoint = "primary"
	EndpointSecondary Endpoint = "secondary"
)

// Rule defines a routing rule for a specific bucket/prefix combination.
type Rule struct {
	Bucket  string            `yaml:"bucket"`  // logical bucket name
	Prefix  string            `yaml:"prefix"`  // Prefix within the bucket ("" means root)
	Actions map[string]Action `yaml:"actions"` // op -> action (must contain "*")
}

// Config is the compiled configuration for the S3 router.
type Config struct {
	Endpoints map[Endpoint]string      `yaml:"endpoints"`
	Buckets   map[string]BucketMapping `yaml:"buckets"`
	Rules     []Rule                   `yaml:"rules"`
}

// Load reads configuration from the given reader and returns a compiled Config.
func Load(r io.Reader) (*Config, error) {
	var yml yamlConfig
	dec := yaml.NewDecoder(r)
	if err := dec.Decode(&yml); err != nil {
		return nil, err
	}

	endpoints := make(map[Endpoint]string, len(yml.Endpoints))
	for k, v := range yml.Endpoints {
		endpoints[Endpoint(k)] = v
	}
	cfg := &Config{
		Endpoints: endpoints,
		Buckets:   yml.Buckets,
		Rules:     make([]Rule, 0, len(yml.Rules)),
	}

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
	for _, rule := range cfg.Rules {
		if rule.Bucket != bucket && rule.Bucket != "*" {
			continue
		}
		if rule.Prefix != "" && !strings.HasPrefix(key, rule.Prefix) {
			continue
		}
		if act, ok := rule.Actions[op]; ok {
			return rule, act
		}
		return rule, rule.Actions["*"]
	}
	return Rule{}, ActPrimary
}

// IsLogicalBucket returns true if the given bucket name is a logical bucket defined in the configuration.
func (cfg *Config) IsLogicalBucket(bucket string) bool {
	_, ok := cfg.Buckets[bucket]
	return ok
}

// PhysicalBuckets returns the primary and secondary physical bucket names for the given logical bucket.
func (cfg *Config) PhysicalBuckets(logical string) (string, string) {
	if m, ok := cfg.Buckets[logical]; ok {
		return m.Primary, m.Secondary
	}
	return logical, logical
}
