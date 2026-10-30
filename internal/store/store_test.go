package store

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func open(t *testing.T) *Store {
	s, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	s.Database = "mds"
	t.Cleanup(func() { s.Close() })
	return s
}

func ev(lsn uint64, idx int, op, schema, table, row string, at time.Time) Event {
	return Event{Op: op, Schema: schema, Table: table, Key: json.RawMessage(`{"id":"k"}`),
		Row: json.RawMessage(row), At: at, LSN: "0/1", Xid: 7, CommitLSN: lsn, TxIndex: idx}
}

func conds(t *testing.T, ss ...string) []Condition {
	var out []Condition
	for _, s := range ss {
		c, err := ParseCondition(s)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
	}
	return out
}

func TestParseCondition(t *testing.T) {
	good := map[string]Condition{
		"status=VEHICLE_IN": {"status", "=", "VEHICLE_IN"},
		"weight_kg>=1000":   {"weight_kg", ">=", "1000"},
		"n<5":               {"n", "<", "5"},
		"deleted_at!=null":  {"deleted_at", "!=", "null"},
		"meta.gate=1":       {"meta.gate", "=", "1"},
		"vehicle_no~BR*":    {"vehicle_no", "~", "BR*"},
		"note=a=b>c":        {"note", "=", "a=b>c"},
		"x=":                {"x", "=", ""},
	}
	for in, want := range good {
		got, err := ParseCondition(in)
		if err != nil || got != want {
			t.Errorf("%q: got %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"status", "=x", "a b=1", "a') or 1=1 --=x", "a..b=1"} {
		if _, err := ParseCondition(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

func TestAppendQueryAndReplayIsIdempotent(t *testing.T) {
	s, ctx := open(t), context.Background()
	now := time.Now()
	tx := []Event{
		ev(100, 0, "insert", "order", "parking_logs", `{"id":"p1","status":"VEHICLE_IN","n":10,"ok":true,"meta":{"gate":2},"gone":null}`, now),
		ev(100, 1, "update", "order", "orders", `{"id":"o1","status":"OPEN","n":3,"amount":"1200.50","code":"abc"}`, now),
		ev(100, 2, "delete", "procurement", "trucks", `null`, now),
	}
	tx[1].Before = json.RawMessage(`{"id":"o1","status":"NEW","n":3}`)
	tx[1].Changed = []string{"status"}
	if err := s.Append(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(ctx, tx); err != nil { // replay of the same transaction
		t.Fatal(err)
	}
	n, _ := s.Count(ctx)
	if n != 3 {
		t.Fatalf("replay duplicated rows: count=%d", n)
	}

	all, _ := s.Query(ctx, Query{})
	if len(all) != 3 || all[0].ID != 1 || all[2].ID != 3 {
		t.Fatalf("ids not sequential: %+v", all)
	}
	if all[0].Database != "mds" || string(all[2].Row) != "null" || all[0].Before != nil {
		t.Fatalf("bad event %+v", all[2])
	}
	if string(all[1].Before) != `{"id":"o1","status":"NEW","n":3}` || len(all[1].Changed) != 1 {
		t.Fatalf("before/changed lost: %+v", all[1])
	}

	cases := []struct {
		q    Query
		want int
	}{
		{Query{After: 1}, 2},
		{Query{Tables: []string{"order.*"}}, 2},
		{Query{Tables: []string{"order.orders", "procurement.trucks"}}, 2},
		{Query{Tables: []string{"*.tr*"}}, 1},
		{Query{Ops: []string{"delete"}}, 1},
		{Query{Where: conds(t, "status=VEHICLE_IN")}, 1},
		{Query{Where: conds(t, "n=10")}, 1},
		{Query{Where: conds(t, "n>=3")}, 2},
		{Query{Where: conds(t, "n<10")}, 1},
		{Query{Where: conds(t, "ok=true")}, 1},
		{Query{Where: conds(t, "meta.gate=2")}, 1},
		{Query{Where: conds(t, "status~VEH*")}, 1},
		{Query{Where: conds(t, "status!=OPEN")}, 2}, // includes the delete, whose row is null
		{Query{Where: conds(t, "gone=null")}, 3},
		{Query{Where: conds(t, "status!=null")}, 2},
		{Query{Changed: []string{"status", "amount"}}, 1},
		{Query{Where: conds(t, "amount>=100")}, 1}, // numeric string compared as a number
		{Query{Where: conds(t, "amount>=2000")}, 0},
		{Query{Where: conds(t, "amount=1200.5")}, 1},  // "1200.50" == 1200.5
		{Query{Where: conds(t, "amount!=1200.5")}, 2}, // the other two rows (one null row)
		{Query{Where: conds(t, "code=0")}, 0},         // a non-numeric string never equals a number
		{Query{Where: conds(t, "n>2.5")}, 2},
		{Query{Tables: []string{"order.*"}, Ops: []string{"insert"}, Where: conds(t, "status=OPEN")}, 0},
	}
	for _, c := range cases {
		got, err := s.Query(ctx, c.q)
		if err != nil || len(got) != c.want {
			t.Errorf("%+v: got %d events, err %v", c.q, len(got), err)
		}
	}
	if _, err := s.Query(ctx, Query{Where: []Condition{{"a') or 1=1 --", "=", "x"}}}); err == nil {
		t.Fatal("bad column name accepted")
	}
}

func TestSweepAndConsumers(t *testing.T) {
	s, ctx := open(t), context.Background()
	old := time.Now().Add(-40 * 24 * time.Hour)
	_ = s.Append(ctx, []Event{ev(1, 0, "insert", "s", "t", `{"a":1}`, old), ev(2, 0, "insert", "s", "t", `{"a":2}`, time.Now())})

	deleted, err := s.Sweep(ctx, 30, 0)
	if err != nil || deleted != 1 {
		t.Fatalf("sweep by days: deleted=%d err=%v", deleted, err)
	}
	oldest, newest, _ := s.Bounds(ctx)
	if oldest != 2 || newest != 2 {
		t.Fatalf("bounds %d %d", oldest, newest)
	}

	if f, _ := s.Floor(ctx); f != 1 {
		t.Fatalf("floor with oldest=2 should be 1, got %d", f)
	}
	// Size cap: 1 byte forces everything out.
	deleted, err = s.Sweep(ctx, 0, 1)
	if err != nil || deleted != 1 {
		t.Fatalf("sweep by size: deleted=%d err=%v", deleted, err)
	}
	// Empty store still knows that ids up to 2 once existed.
	if f, _ := s.Floor(ctx); f != 2 {
		t.Fatalf("floor of an emptied store should be the last id (2), got %d", f)
	}
	if f, _ := open(t).Floor(ctx); f != 0 {
		t.Fatalf("floor of a fresh store should be 0, got %d", f)
	}

	if err := s.SetConsumer(ctx, "bot", 10); err != nil {
		t.Fatal(err)
	}
	_ = s.SetConsumer(ctx, "bot", 5) // never moves backwards
	c, err := s.GetConsumer(ctx, "bot")
	if err != nil || c.Position != 10 {
		t.Fatalf("consumer %+v %v", c, err)
	}
	if _, err := s.GetConsumer(ctx, "nobody"); err != ErrNotFound {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
	list, _ := s.ListConsumers(ctx)
	if len(list) != 1 {
		t.Fatal("list")
	}
}

func TestSubscribeWakes(t *testing.T) {
	s, ctx := open(t), context.Background()
	ch, stop := s.Subscribe()
	defer stop()
	_ = s.Append(ctx, []Event{ev(1, 0, "insert", "s", "t", `{}`, time.Now())})
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("no wake-up after append")
	}
}
