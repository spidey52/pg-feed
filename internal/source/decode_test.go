package source

import (
	"testing"

	"github.com/jackc/pglogrepl"
)

func col(name string, oid uint32, keyCol bool) *pglogrepl.RelationMessageColumn {
	c := &pglogrepl.RelationMessageColumn{Name: name, DataType: oid}
	if keyCol {
		c.Flags = 1
	}
	return c
}

func text(s string) *pglogrepl.TupleDataColumn {
	return &pglogrepl.TupleDataColumn{DataType: pglogrepl.TupleDataTypeText, Data: []byte(s)}
}

var rel = &pglogrepl.RelationMessage{Namespace: "order", RelationName: "parking_logs", Columns: []*pglogrepl.RelationMessageColumn{
	col("id", 2950, true), col("weight", oidInt4, true), col("ok", oidBool, true), col("meta", oidJSONB, true),
	col("amount", 1700, true), col("note", 25, true), col("blob", 25, true), col("at", oidTimestampTZ, true), col("local", oidTimestamp, true),
}}

func TestRowKeyChanged(t *testing.T) {
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{
		text("p1"), text("1200"), text("t"), text(`{"a":[1,2]}`), text("12.50"), {DataType: pglogrepl.TupleDataTypeNull},
		{DataType: pglogrepl.TupleDataTypeToast}, text("2026-10-29 18:38:40.754081+05:30"), text("2026-10-29 18:38:40.5"),
	}}
	m, unchanged := row(rel, tuple)
	want := `{"amount":"12.50","at":"2026-10-29T13:08:40.754081Z","id":"p1","local":"2026-10-29T18:38:40.5","meta":{"a":[1,2]},"note":null,"ok":true,"weight":1200}`
	if got := string(marshal(m)); got != want {
		t.Fatalf("row =\n%s\nwant\n%s", got, want)
	}
	if len(unchanged) != 1 || unchanged[0] != "blob" {
		t.Fatalf("unchanged = %v", unchanged)
	}
	// Primary key known: only those columns. Unknown: every flagged column (here: all).
	if k := key(rel, m, []string{"id"}); string(k) != `{"id":"p1"}` {
		t.Fatalf("key = %s", k)
	}
	if k := key(rel, m, nil); len(k) < 50 {
		t.Fatalf("fallback key should hold all identity columns: %s", k)
	}
	if r, _ := row(rel, nil); r != nil || marshal(r) != nil {
		t.Fatal("nil tuple must give nil row")
	}

	before := map[string]any{"id": "p1", "weight": value(oidInt4, "1000"), "ok": true, "note": nil}
	after := map[string]any{"id": "p1", "weight": value(oidInt4, "1200"), "ok": false, "note": nil}
	if c := changed(rel, before, after); len(c) != 2 || c[0] != "weight" || c[1] != "ok" {
		t.Fatalf("changed = %v", c)
	}
}

func TestPublicationFor(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"*"}, " for all tables"},
		{[]string{"a.b", "*"}, " for all tables"},
		{[]string{"a.b", "c.*", "d.e"}, ` for table "a"."b", "d"."e", tables in schema "c"`},
		{[]string{"order.*"}, ` for tables in schema "order"`},
	}
	for _, c := range cases {
		if got := publicationFor(c.in); got != c.want {
			t.Errorf("publicationFor(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestValueKeepsOddText(t *testing.T) {
	for _, tc := range []struct {
		oid  uint32
		in   string
		want any
	}{
		{oidFloat8, "NaN", "NaN"},
		{oidTimestampTZ, "infinity", "infinity"},
		{oidInt8, "9223372036854775807", "9223372036854775807"},
		{oidJSON, "not json", "not json"},
		{1082, "2026-10-29", "2026-10-29"},
	} {
		got := value(tc.oid, tc.in)
		if s, ok := got.(string); ok && s == tc.want {
			continue
		}
		if r, ok := got.([]byte); ok && string(r) == tc.want {
			continue
		}
		if r, ok := got.(interface{ MarshalJSON() ([]byte, error) }); ok {
			if b, _ := r.MarshalJSON(); string(b) == tc.want {
				continue
			}
		}
		t.Errorf("value(%d, %q) = %#v", tc.oid, tc.in, got)
	}
}
