// Package config reads the YAML config file. ${VAR} inside the file is
// replaced from the environment, so secrets never have to be written down.
package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source struct {
		URL             string   `yaml:"url"`
		Slot            string   `yaml:"slot"`
		Publication     string   `yaml:"publication"`
		CreateIfMissing bool     `yaml:"create_if_missing"`
		Tables          []string `yaml:"tables"`
	} `yaml:"source"`
	Store struct {
		Path      string `yaml:"path"`
		Retention struct {
			Days       int           `yaml:"days"`
			MaxSize    Size          `yaml:"max_size"`
			SweepEvery time.Duration `yaml:"sweep_every"`
		} `yaml:"retention"`
	} `yaml:"store"`
	Server struct {
		Listen      string   `yaml:"listen"`
		Tokens      string   `yaml:"tokens"` // comma separated
		CORSOrigins []string `yaml:"cors_origins"`
	} `yaml:"server"`
	Log struct {
		Level string `yaml:"level"` // debug | info | warn | error
	} `yaml:"log"`
}

// Size is a byte count written like "10GB", "500MB" or "1073741824".
type Size int64

func (s *Size) UnmarshalYAML(n *yaml.Node) error {
	v, err := ParseSize(n.Value)
	if err != nil {
		return err
	}
	*s = v
	return nil
}

func ParseSize(raw string) (Size, error) {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	units := []struct {
		suffix string
		mult   int64
	}{{"TB", 1 << 40}, {"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1}}
	mult := int64(1)
	for _, u := range units {
		if strings.HasSuffix(raw, u.suffix) {
			raw = strings.TrimSpace(strings.TrimSuffix(raw, u.suffix))
			mult = u.mult
			break
		}
	}
	n, err := strconv.ParseFloat(raw, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("bad size %q", raw)
	}
	return Size(n * float64(mult)), nil
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	// Unknown variables become empty strings, which validation below catches.
	expanded := expand(string(raw))

	var c Config
	if err := yaml.Unmarshal([]byte(expanded), &c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	// Defaults.
	if c.Source.Slot == "" {
		c.Source.Slot = "pg_feed"
	}
	if c.Source.Publication == "" {
		c.Source.Publication = "pg_feed"
	}
	if len(c.Source.Tables) == 0 {
		c.Source.Tables = []string{"*"}
	}
	if c.Store.Path == "" {
		c.Store.Path = "./data/pg-feed.db"
	}
	if c.Store.Retention.Days == 0 {
		c.Store.Retention.Days = 30
	}
	if c.Store.Retention.SweepEvery == 0 {
		c.Store.Retention.SweepEvery = 10 * time.Minute
	}
	if c.Server.Listen == "" {
		c.Server.Listen = ":9040"
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	// Validation.
	if c.Source.URL == "" {
		return nil, fmt.Errorf("source.url is required")
	}
	for _, t := range c.Source.Tables {
		if t != "*" && !strings.Contains(t, ".") {
			return nil, fmt.Errorf("source.tables entry %q must be schema.table, schema.* or *", t)
		}
		schema, table, _ := strings.Cut(t, ".")
		if c.Source.CreateIfMissing && t != "*" && (strings.ContainsAny(schema, "*?") || (table != "*" && strings.ContainsAny(table, "*?"))) {
			return nil, fmt.Errorf("source.tables entry %q: a publication cannot be created from a wildcard; use schema.table, schema.* or *, or create it by hand", t)
		}
	}
	switch c.Log.Level {
	case "debug", "info", "warn", "error":
	default:
		return nil, fmt.Errorf("log.level %q must be debug, info, warn or error", c.Log.Level)
	}
	return &c, nil
}

var variable = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// expand replaces ${NAME} with the environment variable. Only that form: a
// bare $ (as in a password) is left alone.
func expand(s string) string {
	return variable.ReplaceAllStringFunc(s, func(m string) string {
		return os.Getenv(m[2 : len(m)-1])
	})
}

// Tokens returns the bearer tokens; empty means no auth.
func (c *Config) Tokens() []string {
	var out []string
	for _, t := range strings.Split(c.Server.Tokens, ",") {
		if t = strings.TrimSpace(t); t != "" {
			out = append(out, t)
		}
	}
	return out
}
