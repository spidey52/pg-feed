// Package api is the consumer-facing side: pull events over HTTP, tail them
// over SSE or WebSocket, and remember named consumers' positions. Every
// reader is the same thing: a loop over store.Query that sleeps until the
// store says a new transaction landed.
//
// Handlers are plain net/http, so the whole API is one http.Handler that can
// also be mounted inside another Go server.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/spidey52/pg-feed/internal/source"
	"github.com/spidey52/pg-feed/internal/store"
)

type Options struct {
	Tokens      []string // bearer tokens; empty = no auth
	CORSOrigins []string // allowed Origin values, or "*"
	Version     string
	Retention   Retention
}

type Retention struct {
	Days    int   `json:"days"`
	MaxSize int64 `json:"maxSizeBytes"`
}

type Server struct {
	store  *store.Store
	source *source.Source
	opt    Options
	tokens map[string]bool
	log    *slog.Logger
}

func New(st *store.Store, src *source.Source, opt Options, log *slog.Logger) http.Handler {
	s := &Server{store: st, source: src, opt: opt, tokens: map[string]bool{}, log: log}
	for _, t := range opt.Tokens {
		s.tokens[t] = true
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /v1/status", s.auth(s.status))
	mux.HandleFunc("GET /v1/events", s.auth(s.events))
	mux.HandleFunc("GET /v1/stream", s.auth(s.stream))
	mux.HandleFunc("GET /v1/watch", s.auth(s.watch))
	mux.HandleFunc("GET /v1/consumers", s.auth(s.listConsumers))
	mux.HandleFunc("GET /v1/consumers/{name}", s.auth(s.getConsumer))
	mux.HandleFunc("PUT /v1/consumers/{name}", s.auth(s.putConsumer))
	mux.HandleFunc("DELETE /v1/consumers/{name}", s.auth(s.deleteConsumer))
	return s.cors(mux)
}

func (s *Server) cors(next http.Handler) http.Handler {
	if len(s.opt.CORSOrigins) == 0 {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		for _, o := range s.opt.CORSOrigins {
			if o == "*" || o == origin {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Last-Event-ID")
				w.Header().Set("Access-Control-Allow-Methods", "GET, PUT, DELETE, OPTIONS")
				w.Header().Set("Vary", "Origin")
				break
			}
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if len(s.tokens) > 0 {
			tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if tok == "" {
				tok = r.URL.Query().Get("token") // browsers' EventSource / WebSocket cannot set headers
			}
			if !s.tokens[tok] {
				writeErr(w, http.StatusUnauthorized, "unauthorized", "missing or unknown bearer token")
				return
			}
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, kind, msg string) {
	writeJSON(w, code, map[string]string{"error": kind, "message": msg})
}

type apiError struct {
	code int
	kind string
	msg  string
}

func (e *apiError) Error() string { return e.msg }

func fail(w http.ResponseWriter, err error) {
	var ae *apiError
	if errors.As(err, &ae) {
		writeErr(w, ae.code, ae.kind, ae.msg)
		return
	}
	writeErr(w, http.StatusInternalServerError, "internal", err.Error())
}

// Subscription is what a consumer asks for: which events, and from where.
// It is read from query parameters (pull, SSE) or the WebSocket's first message.
type Subscription struct {
	Tables  []string `json:"tables"`
	Ops     []string `json:"ops"`
	Where   []string `json:"where"`   // "field<op>value" conditions on the row
	Changed []string `json:"changed"` // only updates where one of these columns changed

	After    int64  `json:"after"`    // start after this id
	From     string `json:"from"`     // "now" = only new events
	FromTime string `json:"fromTime"` // RFC 3339: events committed at or after
	Consumer string `json:"consumer"` // start after this consumer's saved position
}

func (sub Subscription) query(after int64, limit int) (store.Query, error) {
	q := store.Query{After: after, Tables: sub.Tables, Ops: sub.Ops, Changed: sub.Changed, Limit: limit}
	for _, w := range sub.Where {
		c, err := store.ParseCondition(w)
		if err != nil {
			return q, &apiError{400, "bad_request", err.Error()}
		}
		q.Where = append(q.Where, c)
	}
	for _, op := range sub.Ops {
		switch op {
		case "insert", "update", "delete", "truncate":
		default:
			return q, &apiError{400, "bad_request", fmt.Sprintf("unknown op %q", op)}
		}
	}
	return q, nil
}

func subscriptionFromQuery(r *http.Request) Subscription {
	q := r.URL.Query()
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	sub := Subscription{
		Tables: splitList(q["table"]), Ops: splitList(q["op"]), Where: q["where"], Changed: splitList(q["changed"]),
		After: after, From: q.Get("from"), FromTime: q.Get("from_time"), Consumer: q.Get("consumer"),
	}
	// An SSE reconnect resumes exactly where the client stopped, whatever the
	// URL said about where to start the first time.
	if id, err := strconv.ParseInt(r.Header.Get("Last-Event-ID"), 10, 64); err == nil {
		sub.After, sub.From, sub.FromTime, sub.Consumer = id, "", "", ""
	}
	return sub
}

func splitList(values []string) []string {
	var out []string
	for _, v := range values {
		for _, p := range strings.Split(v, ",") {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
	}
	return out
}

// resolveStart turns the subscription's start into an id to read after.
func (s *Server) resolveStart(ctx context.Context, sub Subscription) (int64, error) {
	_, newest, err := s.store.Bounds(ctx)
	if err != nil {
		return 0, err
	}
	after := sub.After
	switch {
	case sub.Consumer != "":
		c, err := s.store.GetConsumer(ctx, sub.Consumer)
		if err == nil {
			after = c.Position
		} else if !errors.Is(err, store.ErrNotFound) {
			return 0, err
		}
	case sub.FromTime != "":
		t, err := time.Parse(time.RFC3339, sub.FromTime)
		if err != nil {
			return 0, &apiError{400, "bad_request", "from_time must be RFC 3339, e.g. 2026-10-29T10:00:00Z"}
		}
		if after, err = s.store.IDAt(ctx, t); err != nil {
			return 0, err
		}
	case sub.From == "now":
		after = newest
	case sub.From != "":
		return 0, &apiError{400, "bad_request", `from must be "now"`}
	}
	// A position older than what we keep cannot be resumed without a gap.
	floor, err := s.store.Floor(ctx)
	if err != nil {
		return 0, err
	}
	if after > 0 && after < floor {
		return 0, &apiError{410, "cursor_expired", fmt.Sprintf("events after %d were deleted by retention; the oldest position that can be resumed is %d", after, floor)}
	}
	return after, nil
}

// Status, health, metrics ---------------------------------------------------

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	if !s.source.Status().Connected {
		writeErr(w, http.StatusServiceUnavailable, "not_ready", "not connected to Postgres")
		return
	}
	w.Write([]byte("ok"))
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	oldest, newest, _ := s.store.Bounds(ctx)
	count, _ := s.store.Count(ctx)
	size, _ := s.store.Size(ctx)
	writeJSON(w, 200, map[string]any{
		"version": s.opt.Version,
		"source":  s.source.Status(),
		"store": map[string]any{
			"oldestId": oldest, "newestId": newest, "events": count, "sizeBytes": size, "retention": s.opt.Retention,
		},
	})
}

// metrics is Prometheus text format, hand-written to avoid a dependency.
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	st := s.source.Status()
	oldest, newest, _ := s.store.Bounds(ctx)
	count, _ := s.store.Count(ctx)
	size, _ := s.store.Size(ctx)
	connected := 0
	if st.Connected {
		connected = 1
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintf(w, "# TYPE pg_feed_connected gauge\npg_feed_connected %d\n", connected)
	fmt.Fprintf(w, "# TYPE pg_feed_transactions_total counter\npg_feed_transactions_total %d\n", st.Transactions)
	fmt.Fprintf(w, "# TYPE pg_feed_reconnections_total counter\npg_feed_reconnections_total %d\n", st.Reconnections)
	fmt.Fprintf(w, "# TYPE pg_feed_lag_bytes gauge\npg_feed_lag_bytes %d\n", st.LagBytes)
	fmt.Fprintf(w, "# TYPE pg_feed_store_events gauge\npg_feed_store_events %d\n", count)
	fmt.Fprintf(w, "# TYPE pg_feed_store_size_bytes gauge\npg_feed_store_size_bytes %d\n", size)
	fmt.Fprintf(w, "# TYPE pg_feed_store_oldest_id gauge\npg_feed_store_oldest_id %d\n", oldest)
	fmt.Fprintf(w, "# TYPE pg_feed_store_newest_id gauge\npg_feed_store_newest_id %d\n", newest)
	if !st.LastCommitAt.IsZero() {
		fmt.Fprintf(w, "# TYPE pg_feed_last_commit_timestamp_seconds gauge\npg_feed_last_commit_timestamp_seconds %d\n", st.LastCommitAt.Unix())
	}
}

// Events -----------------------------------------------------------------------

// GET /v1/events  (pull)
func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	sub := subscriptionFromQuery(r)
	after, err := s.resolveStart(r.Context(), sub)
	if err != nil {
		fail(w, err)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	q, err := sub.query(after, limit)
	if err != nil {
		fail(w, err)
		return
	}
	events, err := s.store.Query(r.Context(), q)
	if err != nil {
		fail(w, err)
		return
	}
	if events == nil {
		events = []store.Event{}
	}
	next := after
	if n := len(events); n > 0 {
		next = events[n-1].ID
	}
	writeJSON(w, 200, map[string]any{"events": events, "next": next})
}

// follow sends every event matching q, then waits for new ones, until ctx
// ends or send fails. While idle it calls keepalive every 15 s; everything
// happens on this one goroutine, so the connection is never written to twice.
func (s *Server) follow(ctx context.Context, q store.Query, send func(store.Event) error, keepalive func() error) error {
	const batch = 500
	q.Limit = batch
	wake, stop := s.store.Subscribe()
	defer stop()
	idle := time.NewTimer(keepaliveEvery)
	defer idle.Stop()
	for {
		events, err := s.store.Query(ctx, q)
		if err != nil {
			return err
		}
		for _, e := range events {
			if err := send(e); err != nil {
				return err
			}
			q.After = e.ID
		}
		if len(events) == batch {
			continue // more backlog
		}
		select {
		case <-ctx.Done():
			return nil
		case <-wake:
		case <-idle.C:
			if err := keepalive(); err != nil {
				return err
			}
		}
		if !idle.Stop() {
			select {
			case <-idle.C:
			default:
			}
		}
		idle.Reset(keepaliveEvery)
	}
}

const keepaliveEvery = 15 * time.Second

// GET /v1/stream  (SSE; reconnects resume from Last-Event-ID)
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	sub := subscriptionFromQuery(r)
	after, err := s.resolveStart(r.Context(), sub)
	if err != nil {
		fail(w, err)
		return
	}
	q, err := sub.query(after, 0)
	if err != nil {
		fail(w, err)
		return
	}
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeErr(w, 500, "internal", "streaming not supported")
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(200)
	flusher.Flush()

	_ = s.follow(r.Context(), q, func(e store.Event) error {
		b, _ := json.Marshal(e)
		if _, err := fmt.Fprintf(w, "id: %d\nevent: change\ndata: %s\n\n", e.ID, b); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}, func() error { // comment line: keeps proxies and browsers from timing out
		if _, err := fmt.Fprint(w, ": ping\n\n"); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	})
}

// GET /v1/watch (WebSocket). The first client message is the Subscription as
// JSON. Then the server sends {"type":"ready","after":N} followed by
// {"type":"event","event":{...}} messages. The client may send
// {"type":"ack","id":N} to save the position of the named consumer.
func (s *Server) watch(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, s.wsOptions())
	if err != nil {
		return
	}
	defer conn.CloseNow()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	sendErr := func(err error) {
		kind, msg := "internal", err.Error()
		var ae *apiError
		if errors.As(err, &ae) {
			kind = ae.kind
		}
		b, _ := json.Marshal(map[string]string{"type": "error", "error": kind, "message": msg})
		conn.Write(ctx, websocket.MessageText, b)
		conn.Close(websocket.StatusPolicyViolation, kind)
	}

	var sub Subscription
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	_, first, err := conn.Read(rctx)
	rcancel()
	if err != nil || json.Unmarshal(first, &sub) != nil {
		sendErr(&apiError{400, "bad_request", "first message must be the subscription JSON"})
		return
	}
	after, err := s.resolveStart(ctx, sub)
	if err != nil {
		sendErr(err)
		return
	}
	q, err := sub.query(after, 0)
	if err != nil {
		sendErr(err)
		return
	}

	go func() { // acks, and the close frame from the client
		for {
			_, msg, err := conn.Read(ctx)
			if err != nil {
				cancel()
				return
			}
			var ack struct {
				Type string `json:"type"`
				ID   int64  `json:"id"`
			}
			if json.Unmarshal(msg, &ack) == nil && ack.Type == "ack" && sub.Consumer != "" {
				if err := s.store.SetConsumer(ctx, sub.Consumer, ack.ID); err != nil {
					s.log.Warn("save consumer position", "consumer", sub.Consumer, "error", err)
				}
			}
		}
	}()

	b, _ := json.Marshal(map[string]any{"type": "ready", "after": after})
	if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
		return
	}
	_ = s.follow(ctx, q, func(e store.Event) error {
		b, _ := json.Marshal(map[string]any{"type": "event", "event": e})
		return conn.Write(ctx, websocket.MessageText, b)
	}, func() error { // a ping the client must answer, or the connection is dead
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		return conn.Ping(pctx)
	})
	conn.Close(websocket.StatusNormalClosure, "")
}

// wsOptions applies the CORS origin list to WebSocket upgrades. Browsers send
// an Origin header; without a list only same-origin pages may connect. Other
// clients send no Origin and are always allowed (the token is their check).
func (s *Server) wsOptions() *websocket.AcceptOptions {
	opt := &websocket.AcceptOptions{}
	for _, o := range s.opt.CORSOrigins {
		if o == "*" {
			opt.InsecureSkipVerify = true
			return opt
		}
		host := o
		if u, err := url.Parse(o); err == nil && u.Host != "" {
			host = u.Host
		}
		opt.OriginPatterns = append(opt.OriginPatterns, host)
	}
	return opt
}

// Consumers ----------------------------------------------------------------

func (s *Server) listConsumers(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListConsumers(r.Context())
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, map[string]any{"consumers": list})
}

func (s *Server) getConsumer(w http.ResponseWriter, r *http.Request) {
	c, err := s.store.GetConsumer(r.Context(), r.PathValue("name"))
	if errors.Is(err, store.ErrNotFound) {
		writeErr(w, 404, "not_found", "no such consumer")
		return
	}
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, c)
}

// PUT /v1/consumers/{name}  {"position": 123}  — position only moves forward.
func (s *Server) putConsumer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Position int64 `json:"position"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, "bad_request", `body must be {"position": <id>}`)
		return
	}
	if err := s.store.SetConsumer(r.Context(), r.PathValue("name"), body.Position); err != nil {
		fail(w, err)
		return
	}
	c, err := s.store.GetConsumer(r.Context(), r.PathValue("name"))
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, 200, c)
}

func (s *Server) deleteConsumer(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteConsumer(r.Context(), r.PathValue("name")); err != nil {
		fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
