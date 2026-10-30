// Package store keeps the events in one SQLite file. Ids are the integer
// primary key, so "after id X" is a plain index range, and (commit_lsn,
// tx_index) is unique so a replay after a crash cannot store a row twice.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Event is one row change. It is what consumers receive.
type Event struct {
	ID        int64           `json:"id"`
	Op        string          `json:"op"` // insert | update | delete | truncate
	Database  string          `json:"database"`
	Schema    string          `json:"schema"`
	Table     string          `json:"table"`
	Key       json.RawMessage `json:"key"`                 // primary key columns
	Row       json.RawMessage `json:"row"`                 // the row after the change; null for delete / truncate
	Before    json.RawMessage `json:"before,omitempty"`    // the row before; only with REPLICA IDENTITY FULL
	Changed   []string        `json:"changed,omitempty"`   // columns whose value differs between before and row
	Unchanged []string        `json:"unchanged,omitempty"` // large (TOASTed) columns Postgres did not resend
	At        time.Time       `json:"at"`                  // commit time
	LSN       string          `json:"lsn"`
	Xid       uint32          `json:"xid"`

	CommitLSN uint64 `json:"-"`
	TxIndex   int    `json:"-"`
}

// Condition is one `field op value` test on the row, e.g. status=VEHICLE_IN,
// weight_kg>=1000, vehicle_no~BR*, deleted_at!=null. Fields may be dotted
// paths into JSON columns (meta.gate=1).
type Condition struct {
	Field string
	Op    string
	Value string
}

// Query selects events after an id. Zero values mean "no filter".
type Query struct {
	After   int64
	Tables  []string    // "schema.table", "schema.*" or "*"; globs with * and ?
	Ops     []string    // insert, update, delete, truncate
	Where   []Condition // all must hold, on the row after the change
	Changed []string    // only updates where at least one of these columns changed
	Limit   int
}

type Consumer struct {
	Name      string    `json:"name"`
	Position  int64     `json:"position"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type Store struct {
	db       *sql.DB
	Database string // Postgres database name, put on every event

	mu   sync.Mutex
	subs map[chan struct{}]struct{}
}

const schema = `
create table if not exists events (
  id             integer primary key autoincrement,
  commit_lsn     integer not null,
  tx_index       integer not null,
  op             text    not null,
  schema_name    text    not null,
  table_name     text    not null,
  key_json       text    not null,
  row_json       text,
  before_json    text,
  changed_json   text,
  unchanged_json text,
  committed_at   integer not null,
  lsn            text    not null,
  xid            integer not null,
  unique (commit_lsn, tx_index)
);
create index if not exists events_table_id on events (schema_name, table_name, id);
create index if not exists events_committed_at on events (committed_at);
create table if not exists consumers (
  name       text primary key,
  position   integer not null,
  updated_at integer not null
);
create table if not exists meta (key text primary key, value text not null);
`

func Open(path string) (*Store, error) {
	// synchronous=FULL: the slot is acknowledged right after a commit here,
	// so a commit must survive a power loss too, not only a process crash.
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_pragma=busy_timeout(5000)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// Must run before the tables exist to take effect on a new file.
	if _, err := db.Exec("pragma auto_vacuum = incremental"); err != nil {
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return &Store{db: db, subs: map[chan struct{}]struct{}{}}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// Append stores one transaction's events atomically and wakes live readers.
func (s *Store) Append(ctx context.Context, events []Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmt, err := tx.Prepare(`insert or ignore into events
		(commit_lsn, tx_index, op, schema_name, table_name, key_json, row_json, before_json, changed_json, unchanged_json, committed_at, lsn, xid)
		values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for _, e := range events {
		_, err := stmt.Exec(int64(e.CommitLSN), e.TxIndex, e.Op, e.Schema, e.Table,
			rawOrNull(e.Key), rawOrNull(e.Row), rawOrNull(e.Before), listOrNull(e.Changed), listOrNull(e.Unchanged),
			e.At.UnixMilli(), e.LSN, e.Xid)
		if err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	s.wake()
	return nil
}

func rawOrNull(r json.RawMessage) any {
	if len(r) == 0 || string(r) == "null" {
		return nil
	}
	return string(r)
}

func listOrNull(l []string) any {
	if len(l) == 0 {
		return nil
	}
	b, _ := json.Marshal(l)
	return string(b)
}

// Filter grammar -----------------------------------------------------------

var fieldPath = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)

// Operators, longest first where they share a prefix.
var operators = []string{">=", "<=", "!=", "=", ">", "<", "~"}

// ParseCondition reads "field<op>value". The operator is the first one found,
// so values may contain operator characters (note~a=b).
func ParseCondition(s string) (Condition, error) {
	at, op := -1, ""
	for _, o := range operators {
		i := strings.Index(s, o)
		if i < 0 {
			continue
		}
		if at == -1 || i < at || (i == at && len(o) > len(op)) {
			at, op = i, o
		}
	}
	if at <= 0 {
		return Condition{}, fmt.Errorf("condition %q must be field<op>value, op one of %s", s, strings.Join(operators, " "))
	}
	c := Condition{Field: s[:at], Op: op, Value: s[at+len(op):]}
	if !fieldPath.MatchString(c.Field) {
		return Condition{}, fmt.Errorf("bad field %q in condition %q", c.Field, s)
	}
	return c, nil
}

func (c Condition) sql(args *[]any) string {
	x := "json_extract(row_json, '$." + c.Field + "')"
	tv := typed(c.Value)
	_, isInt := tv.(int64)
	_, isFloat := tv.(float64)
	// Numbers also compare against numeric *strings* (Postgres numeric comes
	// through as text to keep precision): cast when the stored value looks
	// like a number, so "amount>=100" works on "1200.50".
	looksNumeric := "(" + x + " glob '[0-9]*' or " + x + " glob '-[0-9]*')"
	asNumber := "(" + looksNumeric + " and cast(" + x + " as real) %s ?)"
	switch {
	case c.Value == "null" && c.Op == "=":
		return x + " is null"
	case c.Value == "null" && c.Op == "!=":
		return x + " is not null"
	case c.Op == "=":
		// Two forms so text and typed JSON values both match ("10" vs 10, "true" vs 1).
		*args = append(*args, c.Value, tv)
		if isInt || isFloat {
			*args = append(*args, tv)
			return "(" + x + " in (?, ?) or " + fmt.Sprintf(asNumber, "=") + ")"
		}
		return x + " in (?, ?)"
	case c.Op == "!=":
		*args = append(*args, c.Value, tv)
		if isInt || isFloat {
			*args = append(*args, tv)
			return "(" + x + " is null or (" + x + " not in (?, ?) and not " + fmt.Sprintf(asNumber, "=") + "))"
		}
		return "(" + x + " is null or " + x + " not in (?, ?))"
	case c.Op == "~":
		*args = append(*args, c.Value)
		return x + " glob ?"
	default: // > >= < <=
		*args = append(*args, tv)
		if isInt || isFloat {
			return fmt.Sprintf(asNumber, c.Op)
		}
		return x + " " + c.Op + " ?"
	}
}

func typed(v string) any {
	switch v {
	case "true":
		return 1
	case "false":
		return 0
	}
	if n, err := strconv.ParseInt(v, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		return f
	}
	return v
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// Query returns up to q.Limit events with id > q.After, oldest first.
func (s *Store) Query(ctx context.Context, q Query) ([]Event, error) {
	where := []string{"id > ?"}
	args := []any{q.After}

	if len(q.Tables) > 0 {
		var parts []string
		for _, t := range q.Tables {
			schemaName, table, _ := strings.Cut(t, ".")
			switch {
			case t == "*" || t == "*.*":
				parts = append(parts, "1")
			case table == "*" && !strings.ContainsAny(schemaName, "*?"):
				parts = append(parts, "schema_name = ?")
				args = append(args, schemaName)
			case !strings.ContainsAny(t, "*?"):
				parts = append(parts, "(schema_name = ? and table_name = ?)")
				args = append(args, schemaName, table)
			default:
				parts = append(parts, "(schema_name || '.' || table_name) glob ?")
				args = append(args, t)
			}
		}
		where = append(where, "("+strings.Join(parts, " or ")+")")
	}
	if len(q.Ops) > 0 {
		where = append(where, "op in ("+placeholders(len(q.Ops))+")")
		for _, op := range q.Ops {
			args = append(args, op)
		}
	}
	for _, c := range q.Where {
		if !fieldPath.MatchString(c.Field) {
			return nil, fmt.Errorf("bad field %q", c.Field)
		}
		where = append(where, c.sql(&args))
	}
	if len(q.Changed) > 0 {
		where = append(where, "exists (select 1 from json_each(changed_json) where value in ("+placeholders(len(q.Changed))+"))")
		for _, col := range q.Changed {
			args = append(args, col)
		}
	}
	limit := q.Limit
	if limit <= 0 || limit > 5000 {
		limit = 500
	}
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, `select id, op, schema_name, table_name, key_json, row_json, before_json,
		changed_json, unchanged_json, committed_at, lsn, xid, commit_lsn, tx_index from events where `+
		strings.Join(where, " and ")+" order by id limit ?", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Event
	for rows.Next() {
		var e Event
		var key, row, before, changed, unchanged sql.NullString
		var at int64
		if err := rows.Scan(&e.ID, &e.Op, &e.Schema, &e.Table, &key, &row, &before, &changed, &unchanged,
			&at, &e.LSN, &e.Xid, &e.CommitLSN, &e.TxIndex); err != nil {
			return nil, err
		}
		e.Database = s.Database
		e.Key = raw(key)
		e.Row = raw(row)
		if before.Valid {
			e.Before = json.RawMessage(before.String)
		}
		if changed.Valid {
			_ = json.Unmarshal([]byte(changed.String), &e.Changed)
		}
		if unchanged.Valid {
			_ = json.Unmarshal([]byte(unchanged.String), &e.Unchanged)
		}
		e.At = time.UnixMilli(at).UTC()
		out = append(out, e)
	}
	return out, rows.Err()
}

func raw(s sql.NullString) json.RawMessage {
	if !s.Valid {
		return json.RawMessage("null")
	}
	return json.RawMessage(s.String)
}

// Bounds returns the oldest and newest id kept (0, 0 when empty).
func (s *Store) Bounds(ctx context.Context) (oldest, newest int64, err error) {
	err = s.db.QueryRowContext(ctx, "select coalesce(min(id), 0), coalesce(max(id), 0) from events").Scan(&oldest, &newest)
	return
}

// Floor is the smallest "after" position that can still be read without a
// gap: one below the oldest kept event, or, when nothing is kept, the last id
// ever assigned (so a position older than deleted events is still refused).
func (s *Store) Floor(ctx context.Context) (int64, error) {
	oldest, _, err := s.Bounds(ctx)
	if err != nil {
		return 0, err
	}
	if oldest > 0 {
		return oldest - 1, nil
	}
	var last int64
	err = s.db.QueryRowContext(ctx, "select coalesce((select seq from sqlite_sequence where name = 'events'), 0)").Scan(&last)
	return last, err
}

// IDAt returns the id to pass as "after" to start from time t: the last id
// committed before t (0 when nothing is older).
func (s *Store) IDAt(ctx context.Context, t time.Time) (int64, error) {
	var id int64
	err := s.db.QueryRowContext(ctx, "select coalesce(max(id), 0) from events where committed_at < ?", t.UnixMilli()).Scan(&id)
	return id, err
}

func (s *Store) Count(ctx context.Context) (n int64, err error) {
	err = s.db.QueryRowContext(ctx, "select count(*) from events").Scan(&n)
	return
}

// Size is the bytes the file uses for data (free pages not counted).
func (s *Store) Size(ctx context.Context) (int64, error) {
	var pageSize, pageCount, freelist int64
	if err := s.db.QueryRowContext(ctx, "pragma page_size").Scan(&pageSize); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, "pragma page_count").Scan(&pageCount); err != nil {
		return 0, err
	}
	if err := s.db.QueryRowContext(ctx, "pragma freelist_count").Scan(&freelist); err != nil {
		return 0, err
	}
	return (pageCount - freelist) * pageSize, nil
}

// Sweep deletes events older than `days` and then, while the file is still
// over maxSize (0 = no limit), the oldest ones. Work is done in small batches
// so the write lock is never held long enough to stall the ingester.
func (s *Store) Sweep(ctx context.Context, days int, maxSize int64) (int64, error) {
	const batch = 5000
	var deleted int64
	step := func(sql string, args ...any) (bool, error) {
		res, err := s.db.ExecContext(ctx, sql, args...)
		if err != nil {
			return false, err
		}
		n, _ := res.RowsAffected()
		deleted += n
		if n > 0 {
			// Return some free pages to the filesystem; a bounded amount per step.
			if _, err := s.db.ExecContext(ctx, "pragma incremental_vacuum(2000)"); err != nil {
				return false, err
			}
		}
		return n == batch, nil
	}
	if days > 0 {
		cutoff := time.Now().Add(-time.Duration(days) * 24 * time.Hour).UnixMilli()
		for more := true; more; {
			var err error
			if more, err = step("delete from events where id in (select id from events where committed_at < ? order by id limit ?)", cutoff, batch); err != nil {
				return deleted, err
			}
		}
	}
	for maxSize > 0 {
		size, err := s.Size(ctx)
		if err != nil || size <= maxSize {
			break
		}
		if _, err := step("delete from events where id in (select id from events order by id limit ?)", batch); err != nil {
			return deleted, err
		}
		if n, _ := s.Count(ctx); n == 0 {
			break
		}
	}
	return deleted, nil
}

// Consumers --------------------------------------------------------------

var ErrNotFound = errors.New("not found")

func (s *Store) GetConsumer(ctx context.Context, name string) (Consumer, error) {
	var c Consumer
	var at int64
	err := s.db.QueryRowContext(ctx, "select name, position, updated_at from consumers where name = ?", name).
		Scan(&c.Name, &c.Position, &at)
	if errors.Is(err, sql.ErrNoRows) {
		return c, ErrNotFound
	}
	c.UpdatedAt = time.UnixMilli(at).UTC()
	return c, err
}

// SetConsumer records a consumer's position; it only ever moves forward.
func (s *Store) SetConsumer(ctx context.Context, name string, position int64) error {
	_, err := s.db.ExecContext(ctx, `insert into consumers (name, position, updated_at) values (?, ?, ?)
		on conflict (name) do update set position = max(position, excluded.position), updated_at = excluded.updated_at`,
		name, position, time.Now().UnixMilli())
	return err
}

func (s *Store) DeleteConsumer(ctx context.Context, name string) error {
	_, err := s.db.ExecContext(ctx, "delete from consumers where name = ?", name)
	return err
}

func (s *Store) ListConsumers(ctx context.Context) ([]Consumer, error) {
	rows, err := s.db.QueryContext(ctx, "select name, position, updated_at from consumers order by name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Consumer{}
	for rows.Next() {
		var c Consumer
		var at int64
		if err := rows.Scan(&c.Name, &c.Position, &at); err != nil {
			return nil, err
		}
		c.UpdatedAt = time.UnixMilli(at).UTC()
		out = append(out, c)
	}
	return out, rows.Err()
}

// Meta ---------------------------------------------------------------------

func (s *Store) GetMeta(ctx context.Context, key string) (string, error) {
	var v string
	err := s.db.QueryRowContext(ctx, "select value from meta where key = ?", key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, "insert into meta (key, value) values (?, ?) on conflict (key) do update set value = excluded.value", key, value)
	return err
}

// Live readers -------------------------------------------------------------

// Subscribe returns a channel that gets a tick after every Append. Call stop
// when done. The channel is buffered by one so ticks never block the writer.
func (s *Store) Subscribe() (ch <-chan struct{}, stop func()) {
	c := make(chan struct{}, 1)
	s.mu.Lock()
	s.subs[c] = struct{}{}
	s.mu.Unlock()
	return c, func() {
		s.mu.Lock()
		delete(s.subs, c)
		s.mu.Unlock()
	}
}

func (s *Store) wake() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.subs {
		select {
		case c <- struct{}{}:
		default:
		}
	}
}
