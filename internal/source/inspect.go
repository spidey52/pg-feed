package source

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Report is what `pg-feed -check` prints: everything the feed needs from
// Postgres, and the problems found.
type Report struct {
	Database           string `json:"database"`
	ServerVersion      string `json:"serverVersion"`
	WALLevel           string `json:"walLevel"`
	MaxReplicationSlot string `json:"maxReplicationSlots"`
	MaxWALSenders      string `json:"maxWalSenders"`
	MaxSlotWALKeepSize string `json:"maxSlotWalKeepSize"`
	Role               struct {
		Name        string `json:"name"`
		Superuser   bool   `json:"superuser"`
		Replication bool   `json:"replication"`
	} `json:"role"`
	Publication struct {
		Name      string `json:"name"`
		Exists    bool   `json:"exists"`
		AllTables bool   `json:"allTables"`
		Tables    int    `json:"tables"`
	} `json:"publication"`
	Slot struct {
		Name          string `json:"name"`
		Exists        bool   `json:"exists"`
		Plugin        string `json:"plugin,omitempty"`
		Active        bool   `json:"active"`
		WALStatus     string `json:"walStatus,omitempty"`
		RetainedBytes int64  `json:"retainedBytes"`
		ConfirmedLSN  string `json:"confirmedLsn,omitempty"`
	} `json:"slot"`
	Problems []string `json:"problems"`
}

// Inspect reads the server settings, role, publication and slot without
// changing anything.
func (s *Source) Inspect(ctx context.Context) (*Report, error) {
	conn, err := pgx.Connect(ctx, s.opt.URL)
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	r := &Report{Problems: []string{}}
	r.Publication.Name = s.opt.Publication
	r.Slot.Name = s.opt.Slot
	problem := func(format string, a ...any) { r.Problems = append(r.Problems, fmt.Sprintf(format, a...)) }

	show := func(name string) string {
		var v string
		_ = conn.QueryRow(ctx, "show "+name).Scan(&v)
		return v
	}
	r.ServerVersion = show("server_version")
	r.WALLevel = show("wal_level")
	r.MaxReplicationSlot = show("max_replication_slots")
	r.MaxWALSenders = show("max_wal_senders")
	r.MaxSlotWALKeepSize = show("max_slot_wal_keep_size")
	if r.WALLevel != "logical" {
		problem("wal_level is %q; set wal_level=logical and restart Postgres", r.WALLevel)
	}
	if r.MaxSlotWALKeepSize == "-1" {
		problem("max_slot_wal_keep_size is -1 (unlimited): a stopped feed can fill the disk with retained WAL")
	}

	err = conn.QueryRow(ctx, `select current_database(), rolname, rolsuper, rolreplication
		from pg_roles where rolname = current_user`).Scan(&r.Database, &r.Role.Name, &r.Role.Superuser, &r.Role.Replication)
	if err != nil {
		return nil, err
	}
	if !r.Role.Superuser && !r.Role.Replication {
		problem("role %q has neither REPLICATION nor SUPERUSER", r.Role.Name)
	}

	err = conn.QueryRow(ctx, `select puballtables, (select count(*) from pg_publication_tables where pubname = $1)
		from pg_publication where pubname = $1`, s.opt.Publication).Scan(&r.Publication.AllTables, &r.Publication.Tables)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if !s.opt.CreateIfMissing {
			problem("publication %q does not exist", s.opt.Publication)
		}
	case err != nil:
		return nil, err
	default:
		r.Publication.Exists = true
		if r.Publication.Tables == 0 {
			problem("publication %q has no tables", s.opt.Publication)
		}
	}

	err = conn.QueryRow(ctx, `select plugin, active, wal_status, coalesce(confirmed_flush_lsn::text, ''),
		coalesce((pg_current_wal_lsn() - restart_lsn)::bigint, 0)
		from pg_replication_slots where slot_name = $1 and database = current_database()`, s.opt.Slot).
		Scan(&r.Slot.Plugin, &r.Slot.Active, &r.Slot.WALStatus, &r.Slot.ConfirmedLSN, &r.Slot.RetainedBytes)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if !s.opt.CreateIfMissing {
			problem("replication slot %q does not exist", s.opt.Slot)
		}
	case err != nil:
		return nil, err
	default:
		r.Slot.Exists = true
		if r.Slot.Plugin != "pgoutput" {
			problem("slot %q uses plugin %s, not pgoutput", s.opt.Slot, r.Slot.Plugin)
		}
		if r.Slot.Active {
			problem("slot %q is in use by another connection", s.opt.Slot)
		}
		if r.Slot.WALStatus == "lost" {
			problem("slot %q has lost WAL (max_slot_wal_keep_size exceeded); drop and recreate it, consumers must resync", s.opt.Slot)
		}
	}
	return r, nil
}
