package source

import (
	"encoding/json"
	"reflect"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
)

// Postgres type OIDs that get a JSON type. Everything else stays the text
// Postgres sent (uuid, numeric, date, interval, arrays, bytea...).
const (
	oidBool        = 16
	oidInt8        = 20
	oidInt2        = 21
	oidInt4        = 23
	oidOID         = 26
	oidJSON        = 114
	oidFloat4      = 700
	oidFloat8      = 701
	oidTimestamp   = 1114
	oidTimestampTZ = 1184
	oidJSONB       = 3802
)

// row turns a tuple into column name -> value. TOASTed columns that Postgres
// did not resend are left out and their names returned in `unchanged`.
func row(rel *pglogrepl.RelationMessage, t *pglogrepl.TupleData) (m map[string]any, unchanged []string) {
	if t == nil {
		return nil, nil
	}
	m = make(map[string]any, len(rel.Columns))
	for i, col := range rel.Columns {
		if i >= len(t.Columns) {
			break
		}
		c := t.Columns[i]
		switch c.DataType {
		case pglogrepl.TupleDataTypeNull:
			m[col.Name] = nil
		case pglogrepl.TupleDataTypeToast:
			unchanged = append(unchanged, col.Name)
		default:
			m[col.Name] = value(col.DataType, string(c.Data))
		}
	}
	return m, unchanged
}

// key picks the identifying columns out of a row: the primary key when known,
// else the columns pgoutput flags as the replica identity.
func key(rel *pglogrepl.RelationMessage, m map[string]any, pk []string) json.RawMessage {
	out := map[string]any{}
	if len(pk) > 0 {
		for _, name := range pk {
			if v, ok := m[name]; ok {
				out[name] = v
			}
		}
	} else {
		for _, col := range rel.Columns {
			if v, ok := m[col.Name]; ok && col.Flags&1 != 0 {
				out[col.Name] = v
			}
		}
	}
	b, _ := json.Marshal(out)
	return b
}

// changed lists the columns whose value differs between before and after, in
// table column order. Columns missing from either side are skipped.
func changed(rel *pglogrepl.RelationMessage, before, after map[string]any) []string {
	var out []string
	for _, col := range rel.Columns {
		b, okB := before[col.Name]
		a, okA := after[col.Name]
		if okA && okB && !reflect.DeepEqual(a, b) {
			out = append(out, col.Name)
		}
	}
	return out
}

func marshal(m map[string]any) json.RawMessage {
	if m == nil {
		return nil
	}
	b, _ := json.Marshal(m)
	return b
}

func value(oid uint32, text string) any {
	switch oid {
	case oidBool:
		return text == "t"
	case oidInt2, oidInt4, oidInt8, oidOID:
		if _, err := strconv.ParseInt(text, 10, 64); err == nil {
			return json.RawMessage(text)
		}
	case oidFloat4, oidFloat8:
		if _, err := strconv.ParseFloat(text, 64); err == nil && text != "NaN" && text != "Infinity" && text != "-Infinity" {
			return json.RawMessage(text)
		}
	case oidJSON, oidJSONB:
		if json.Valid([]byte(text)) {
			return json.RawMessage(text)
		}
	case oidTimestampTZ:
		for _, layout := range []string{"2006-01-02 15:04:05.999999-07", "2006-01-02 15:04:05.999999-07:00"} {
			if ts, err := time.Parse(layout, text); err == nil {
				return ts.UTC().Format(time.RFC3339Nano)
			}
		}
	case oidTimestamp:
		if ts, err := time.Parse("2006-01-02 15:04:05.999999", text); err == nil {
			return ts.Format("2006-01-02T15:04:05.999999999")
		}
	}
	return text
}
