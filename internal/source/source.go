// Package source reads the Postgres publication through one logical
// replication slot (pgoutput, protocol 1) and stores each committed
// transaction in the store. The slot is acknowledged only after the store
// commit, so a crash replays at most the transactions not yet stored, which
// the store ignores as duplicates.
package source

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/spidey52/pg-feed/internal/store"
)

type Options struct {
	URL             string
	Slot            string
	Publication     string
	CreateIfMissing bool
	Tables          []string // globs on "schema.table"
}

type Status struct {
	Connected     bool      `json:"connected"`
	Database      string    `json:"database"`
	Slot          string    `json:"slot"`
	Publication   string    `json:"publication"`
	StoredLSN     string    `json:"storedLsn"` // last commit written to the store
	AckedLSN      string    `json:"ackedLsn"`  // last position confirmed to Postgres
	ServerLSN     string    `json:"serverLsn"` // where Postgres is
	LagBytes      int64     `json:"lagBytes"`
	Transactions  int64     `json:"transactionsSinceStart"`
	LastCommitAt  time.Time `json:"lastCommitAt"`
	LastError     string    `json:"lastError,omitempty"`
	LastErrorAt   time.Time `json:"lastErrorAt,omitempty"`
	ConnectedAt   time.Time `json:"connectedAt,omitempty"`
	Reconnections int64     `json:"reconnections"`
}

type Source struct {
	opt   Options
	store *store.Store
	log   *slog.Logger

	mu     sync.Mutex
	status Status
}

func New(opt Options, st *store.Store, log *slog.Logger) *Source {
	return &Source{opt: opt, store: st, log: log, status: Status{Slot: opt.Slot, Publication: opt.Publication}}
}

func (s *Source) Status() Status {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

func (s *Source) update(f func(*Status)) {
	s.mu.Lock()
	f(&s.status)
	s.mu.Unlock()
}

// Run streams until ctx ends, reconnecting after errors.
func (s *Source) Run(ctx context.Context) error {
	for {
		err := s.runOnce(ctx)
		if ctx.Err() != nil {
			return nil
		}
		s.log.Error("replication stopped, retrying in 5s", "error", err)
		s.update(func(st *Status) {
			st.Connected = false
			st.LastError = err.Error()
			st.LastErrorAt = time.Now().UTC()
			st.Reconnections++
		})
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(5 * time.Second):
		}
	}
}

// Setup checks (and optionally creates) the publication and slot over a normal
// connection, and refuses a database other than the one the store came from.
func (s *Source) Setup(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, s.opt.URL)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var walLevel string
	if err := conn.QueryRow(ctx, "show wal_level").Scan(&walLevel); err != nil {
		return err
	}
	if walLevel != "logical" {
		return fmt.Errorf("wal_level is %q, must be logical", walLevel)
	}
	// IDENTIFY_SYSTEM over a replication connection: proves the role may
	// replicate and names the cluster, without superuser-only catalog access.
	sysID, dbName, err := s.identify(ctx)
	if err != nil {
		return err
	}
	origin := sysID + "/" + dbName
	stored, err := s.store.GetMeta(ctx, "origin")
	if err != nil {
		return err
	}
	if stored != "" && stored != origin {
		return fmt.Errorf("store was filled from Postgres %s, refusing %s; use a new store file", stored, origin)
	}
	if err := s.store.SetMeta(ctx, "origin", origin); err != nil {
		return err
	}
	s.store.Database = dbName
	s.update(func(st *Status) { st.Database = dbName })

	var n int
	if err := conn.QueryRow(ctx, "select count(*) from pg_publication where pubname = $1", s.opt.Publication).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		if !s.opt.CreateIfMissing {
			return fmt.Errorf("publication %q does not exist (set source.create_if_missing or create it)", s.opt.Publication)
		}
		sql := "create publication " + pgx.Identifier{s.opt.Publication}.Sanitize() + publicationFor(s.opt.Tables)
		if _, err := conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("create publication: %w", err)
		}
		s.log.Info("created publication", "sql", sql)
	}

	var plugin string
	var active bool
	err = conn.QueryRow(ctx, `select plugin, active from pg_replication_slots
		where slot_name = $1 and database = current_database()`, s.opt.Slot).Scan(&plugin, &active)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if !s.opt.CreateIfMissing {
			return fmt.Errorf("replication slot %q does not exist (set source.create_if_missing or create it)", s.opt.Slot)
		}
		if _, err := conn.Exec(ctx, "select pg_create_logical_replication_slot($1, 'pgoutput')", s.opt.Slot); err != nil {
			return fmt.Errorf("create slot: %w", err)
		}
		s.log.Info("created replication slot", "slot", s.opt.Slot)
	case err != nil:
		return err
	case plugin != "pgoutput":
		return fmt.Errorf("slot %q uses plugin %s, not pgoutput", s.opt.Slot, plugin)
	case active:
		return fmt.Errorf("slot %q is already in use by another connection", s.opt.Slot)
	}
	return nil
}

// publicationFor turns the table list into the publication's FOR clause:
// "*" → all tables, "schema.*" → tables in schema (Postgres 15+), else tables.
// Partial globs cannot name tables; config rejects them when creating.
func publicationFor(tables []string) string {
	var names, schemas []string
	for _, t := range tables {
		schema, table, _ := strings.Cut(t, ".")
		switch {
		case t == "*" || t == "*.*":
			return " for all tables"
		case table == "*":
			schemas = append(schemas, pgx.Identifier{schema}.Sanitize())
		default:
			names = append(names, pgx.Identifier{schema, table}.Sanitize())
		}
	}
	var parts []string
	if len(names) > 0 {
		parts = append(parts, "table "+strings.Join(names, ", "))
	}
	if len(schemas) > 0 {
		parts = append(parts, "tables in schema "+strings.Join(schemas, ", "))
	}
	return " for " + strings.Join(parts, ", ")
}

func (s *Source) wanted(schema, table string) bool {
	for _, g := range s.opt.Tables {
		if g == "*" || g == "*.*" {
			return true
		}
		if ok, _ := path.Match(g, schema+"."+table); ok {
			return true
		}
	}
	return false
}

// primaryKey returns the primary key column names of a relation, in order.
func primaryKey(ctx context.Context, conn *pgx.Conn, relationOID uint32) ([]string, error) {
	rows, err := conn.Query(ctx, `select a.attname from pg_index i
		join pg_attribute a on a.attrelid = i.indrelid and a.attnum = any(i.indkey::int2[])
		where i.indrelid = $1 and i.indisprimary
		order by array_position(i.indkey::int2[], a.attnum)`, relationOID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// replicationConn opens a connection in logical replication mode.
func (s *Source) replicationConn(ctx context.Context) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(s.opt.URL)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("replication connection (does the role have REPLICATION?): %w", err)
	}
	return conn, nil
}

// identify returns the cluster's system identifier and the database name.
func (s *Source) identify(ctx context.Context) (sysID, dbName string, err error) {
	conn, err := s.replicationConn(ctx)
	if err != nil {
		return "", "", err
	}
	defer conn.Close(ctx)
	id, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return "", "", err
	}
	return id.SystemID, id.DBName, nil
}

func (s *Source) runOnce(ctx context.Context) error {
	conn, err := s.replicationConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	// A normal connection for primary-key lookups when a table is first seen.
	meta, err := pgx.Connect(ctx, s.opt.URL)
	if err != nil {
		return err
	}
	defer meta.Close(context.Background())

	// 0/0 = continue from the slot's confirmed position.
	err = pglogrepl.StartReplication(ctx, conn, s.opt.Slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + s.opt.Publication + "'"},
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	s.log.Info("replication started", "slot", s.opt.Slot, "publication", s.opt.Publication)
	s.update(func(st *Status) { st.Connected = true; st.LastError = ""; st.ConnectedAt = time.Now().UTC() })

	var (
		relations = map[uint32]*pglogrepl.RelationMessage{}
		pks       = map[uint32][]string{}
		acked     pglogrepl.LSN // everything before this is in the store
		begin     *pglogrepl.BeginMessage
		tx        []store.Event
		txIndex   int // position of the next row message in the transaction, filtered or not
		nextAck   = time.Now().Add(10 * time.Second)
	)
	ack := func() error {
		nextAck = time.Now().Add(10 * time.Second)
		s.update(func(st *Status) { st.AckedLSN = acked.String() })
		return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: acked, WALFlushPosition: acked, WALApplyPosition: acked,
		})
	}
	// On shutdown, tell Postgres what is stored so the next start replays little.
	defer func() {
		if acked > 0 {
			_ = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{
				WALWritePosition: acked, WALFlushPosition: acked, WALApplyPosition: acked,
			})
		}
	}()
	rel := func(id uint32) *pglogrepl.RelationMessage {
		r := relations[id]
		if r == nil || !s.wanted(r.Namespace, r.RelationName) {
			return nil
		}
		return r
	}

	for {
		if time.Now().After(nextAck) {
			if err := ack(); err != nil {
				return err
			}
		}
		rctx, cancel := context.WithDeadline(ctx, nextAck)
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			return err
		}
		data, ok := raw.(*pgproto3.CopyData)
		if !ok {
			if e, isErr := raw.(*pgproto3.ErrorResponse); isErr {
				return fmt.Errorf("postgres: %s", e.Message)
			}
			continue
		}

		switch data.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data.Data[1:])
			if err != nil {
				return err
			}
			// Idle between transactions: everything the server sent is stored,
			// so the slot can move on and free WAL even when our tables are quiet.
			if begin == nil && ka.ServerWALEnd > acked {
				acked = ka.ServerWALEnd
			}
			s.update(func(st *Status) {
				st.ServerLSN = ka.ServerWALEnd.String()
				st.LagBytes = int64(ka.ServerWALEnd - acked)
			})
			if ka.ReplyRequested {
				if err := ack(); err != nil {
					return err
				}
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(data.Data[1:])
			if err != nil {
				return err
			}
			msg, err := pglogrepl.Parse(xld.WALData)
			if err != nil {
				return fmt.Errorf("parse pgoutput: %w", err)
			}
			switch m := msg.(type) {
			case *pglogrepl.RelationMessage:
				relations[m.RelationID] = m
				cols, err := primaryKey(ctx, meta, m.RelationID)
				if err != nil {
					s.log.Warn("primary key lookup failed, using replica identity columns", "table", m.Namespace+"."+m.RelationName, "error", err)
				}
				pks[m.RelationID] = cols

			case *pglogrepl.BeginMessage:
				begin = m
				tx = tx[:0]
				txIndex = 0

			case *pglogrepl.InsertMessage:
				txIndex++
				if r := rel(m.RelationID); r != nil {
					after, unchanged := row(r, m.Tuple)
					tx = append(tx, event(r, "insert", key(r, after, pks[r.RelationID]), after, nil, unchanged, txIndex))
				}

			case *pglogrepl.UpdateMessage:
				txIndex++
				if r := rel(m.RelationID); r != nil {
					after, unchanged := row(r, m.NewTuple)
					before, _ := row(r, m.OldTuple) // 'O' = full old row, 'K' = old key only, else nil
					k := key(r, after, pks[r.RelationID])
					if before != nil {
						k = key(r, before, pks[r.RelationID]) // identify by what the row was
					}
					if m.OldTupleType != 'O' {
						before = nil
					}
					tx = append(tx, event(r, "update", k, after, before, unchanged, txIndex))
				}

			case *pglogrepl.DeleteMessage:
				txIndex++
				if r := rel(m.RelationID); r != nil {
					old, _ := row(r, m.OldTuple)
					var before map[string]any
					if m.OldTupleType == 'O' {
						before = old
					}
					tx = append(tx, event(r, "delete", key(r, old, pks[r.RelationID]), nil, before, nil, txIndex))
				}

			case *pglogrepl.TruncateMessage:
				for _, id := range m.RelationIDs {
					txIndex++
					if r := rel(id); r != nil {
						tx = append(tx, event(r, "truncate", []byte("{}"), nil, nil, nil, txIndex))
					}
				}

			case *pglogrepl.CommitMessage:
				if begin == nil {
					return errors.New("commit without begin")
				}
				for i := range tx {
					tx[i].CommitLSN = uint64(m.CommitLSN)
					tx[i].LSN = m.CommitLSN.String()
					tx[i].At = m.CommitTime.UTC()
					tx[i].Xid = begin.Xid
				}
				if err := s.store.Append(ctx, tx); err != nil {
					return fmt.Errorf("store: %w", err)
				}
				begin = nil
				acked = m.TransactionEndLSN
				s.update(func(st *Status) {
					st.Transactions++
					st.StoredLSN = m.CommitLSN.String()
					if len(tx) > 0 {
						st.LastCommitAt = m.CommitTime.UTC()
					}
				})
				// Ack promptly after a commit, at most about once a second.
				if time.Until(nextAck) < 9*time.Second {
					if err := ack(); err != nil {
						return err
					}
				}
			}
		}
	}
}

// event builds a store.Event. txIndex is the row's position among *all* row
// messages of its transaction, so (commit_lsn, tx_index) stays the same even
// if the table filter changes between runs.
func event(rel *pglogrepl.RelationMessage, op string, k []byte, after, before map[string]any, unchanged []string, txIndex int) store.Event {
	e := store.Event{Op: op, Schema: rel.Namespace, Table: rel.RelationName, Key: k, Row: marshal(after), Before: marshal(before), Unchanged: unchanged, TxIndex: txIndex}
	if op == "update" && before != nil {
		e.Changed = changed(rel, before, after)
	}
	return e
}
