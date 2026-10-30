package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadExpandsEnvAndDefaults(t *testing.T) {
	t.Setenv("TEST_DB_URL", "postgres://u:p@h/db")
	t.Setenv("TEST_TOKENS", "a, b,,")
	p := filepath.Join(t.TempDir(), "c.yaml")
	os.WriteFile(p, []byte(`
source:
  url: ${TEST_DB_URL}
  tables: ["order.*"]
store:
  retention: { max_size: 2GB }
server:
  tokens: ${TEST_TOKENS}
`), 0o644)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Source.URL != "postgres://u:p@h/db" || c.Source.Slot != "pg_feed" || c.Store.Retention.Days != 30 {
		t.Fatalf("%+v", c)
	}
	if int64(c.Store.Retention.MaxSize) != 2<<30 || c.Server.Listen != ":9040" || c.Log.Level != "info" {
		t.Fatalf("%+v", c)
	}
	if got := c.Tokens(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("tokens %v", got)
	}
}

func TestExpandOnlyBraces(t *testing.T) {
	t.Setenv("TEST_PW", "s3cret")
	got := expand("postgres://u:${TEST_PW}@h/db?x=$$y&z=$NOPE&w=${UNSET_VAR_XYZ}")
	if got != "postgres://u:s3cret@h/db?x=$$y&z=$NOPE&w=" {
		t.Fatalf("expand = %q", got)
	}
}

func TestLoadRejectsBadInput(t *testing.T) {
	for name, body := range map[string]string{
		"no url":        "store: {}",
		"bad table":     "source: {url: x, tables: [orders]}",
		"bad level":     "source: {url: x}\nlog: {level: loud}",
		"bad size":      "source: {url: x}\nstore: {retention: {max_size: big}}",
		"glob + create": "source: {url: x, create_if_missing: true, tables: ['public.order_*']}",
	} {
		p := filepath.Join(t.TempDir(), "c.yaml")
		os.WriteFile(p, []byte(body), 0o644)
		if _, err := Load(p); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"10GB": 10 << 30, "500mb": 500 << 20, "1.5 KB": 1536, "4096": 4096, "0": 0} {
		got, err := ParseSize(in)
		if err != nil || int64(got) != want {
			t.Errorf("%q = %d, %v", in, got, err)
		}
	}
}
