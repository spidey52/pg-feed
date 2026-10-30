package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/spidey52/pg-feed/internal/source"
	"github.com/spidey52/pg-feed/internal/store"
)

func setup(t *testing.T) (*store.Store, *httptest.Server) {
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	st.Database = "mds"
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	src := source.New(source.Options{Slot: "s"}, st, log)
	opt := Options{Tokens: []string{"secret"}, CORSOrigins: []string{"*"}, Version: "test", Retention: Retention{Days: 30}}
	srv := httptest.NewServer(New(st, src, opt, log))
	t.Cleanup(func() { srv.Close(); st.Close() })
	return st, srv
}

func add(t *testing.T, st *store.Store, lsn uint64, table, row string) {
	err := st.Append(context.Background(), []store.Event{{
		Op: "insert", Schema: "order", Table: table, Key: json.RawMessage(`{"id":1}`),
		Row: json.RawMessage(row), At: time.Now(), LSN: "0/1", CommitLSN: lsn,
	}})
	if err != nil {
		t.Fatal(err)
	}
}

func get(t *testing.T, url, token string) (int, map[string]any) {
	req, _ := http.NewRequest("GET", url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body map[string]any
	json.NewDecoder(res.Body).Decode(&body)
	return res.StatusCode, body
}

func TestAuthPullAndFilters(t *testing.T) {
	st, srv := setup(t)
	add(t, st, 1, "orders", `{"id":1,"status":"OPEN","amount":10}`)
	add(t, st, 2, "trucks", `{"id":1,"status":"IN","amount":50}`)

	if code, _ := get(t, srv.URL+"/v1/events", ""); code != 401 {
		t.Fatalf("no token: %d", code)
	}
	code, body := get(t, srv.URL+"/v1/events?table=order.orders", "secret")
	if code != 200 || len(body["events"].([]any)) != 1 || body["next"].(float64) != 1 {
		t.Fatalf("pull: %d %v", code, body)
	}
	code, body = get(t, srv.URL+"/v1/events?after=1&where=status=IN&where=amount>=20", "secret")
	if code != 200 || len(body["events"].([]any)) != 1 {
		t.Fatalf("filtered pull: %d %v", code, body)
	}
	if code, body = get(t, srv.URL+"/v1/events?where=status", "secret"); code != 400 || body["error"] != "bad_request" {
		t.Fatalf("bad where: %d %v", code, body)
	}
	if code, _ = get(t, srv.URL+"/v1/events?op=upsert", "secret"); code != 400 {
		t.Fatalf("bad op: %d", code)
	}
	if code, _ = get(t, srv.URL+"/v1/events?from=yesterday", "secret"); code != 400 {
		t.Fatalf("bad from: %d", code)
	}
	code, body = get(t, srv.URL+"/v1/status", "secret")
	if code != 200 || body["version"] != "test" {
		t.Fatalf("status: %d %v", code, body)
	}
	res, _ := http.Get(srv.URL + "/metrics")
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode != 200 || !strings.Contains(string(b), "pg_feed_store_events 2") {
		t.Fatalf("metrics: %d %s", res.StatusCode, b)
	}
	if res, _ := http.Get(srv.URL + "/readyz"); res.StatusCode != 503 {
		t.Fatalf("readyz without a connection should be 503, got %d", res.StatusCode)
	}
	req, _ := http.NewRequest("OPTIONS", srv.URL+"/v1/events", nil)
	req.Header.Set("Origin", "https://app.example")
	res, _ = http.DefaultClient.Do(req)
	if res.StatusCode != 204 || res.Header.Get("Access-Control-Allow-Origin") != "https://app.example" {
		t.Fatalf("cors preflight: %d %v", res.StatusCode, res.Header)
	}
}

func TestCursorExpired(t *testing.T) {
	st, srv := setup(t)
	for i := uint64(1); i <= 3; i++ {
		add(t, st, i, "t", `{}`)
	}
	_, _ = st.Sweep(context.Background(), 0, 1) // drops everything
	add(t, st, 4, "t", `{}`)                    // id 4 is the only one kept
	code, body := get(t, srv.URL+"/v1/events?after=1", "secret")
	if code != 410 || body["error"] != "cursor_expired" {
		t.Fatalf("want 410 cursor_expired, got %d %v", code, body)
	}
	if code, _ := get(t, srv.URL+"/v1/events?after=3", "secret"); code != 200 {
		t.Fatalf("after=3 should still be fine (exactly the oldest-1): %d", code)
	}
	// Everything deleted: an old position is still expired, not silently accepted.
	_, _ = st.Sweep(context.Background(), 0, 1)
	if code, body := get(t, srv.URL+"/v1/events?after=2", "secret"); code != 410 {
		t.Fatalf("empty store, after=2: want 410, got %d %v", code, body)
	}
	if code, _ := get(t, srv.URL+"/v1/events?after=4", "secret"); code != 200 {
		t.Fatalf("empty store, after=4 (the last id): want 200, got %d", code)
	}
}

func TestSSEReconnectResumesFromLastEventID(t *testing.T) {
	st, srv := setup(t)
	for i := uint64(1); i <= 5; i++ {
		add(t, st, i, "t", `{}`)
	}
	// The URL says from=now, but the browser is reconnecting after event 2.
	req, _ := http.NewRequest("GET", srv.URL+"/v1/events?from=now", nil)
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Last-Event-ID", "2")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]any
	json.NewDecoder(res.Body).Decode(&body)
	if n := len(body["events"].([]any)); n != 3 {
		t.Fatalf("want events 3,4,5 after Last-Event-ID 2, got %d: %v", n, body)
	}
}

func TestWebSocketOriginCheck(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	src := source.New(source.Options{}, st, log)
	srv := httptest.NewServer(New(st, src, Options{CORSOrigins: []string{"https://app.example"}}, log))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/watch"

	for origin, ok := range map[string]bool{"https://app.example": true, "https://evil.example": false, "": true} {
		h := http.Header{}
		if origin != "" {
			h.Set("Origin", origin)
		}
		conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPHeader: h})
		if (err == nil) != ok {
			t.Errorf("origin %q: accepted=%v, want %v (%v)", origin, err == nil, ok, err)
		}
		if conn != nil {
			conn.CloseNow()
		}
	}
}

func TestWebSocketLiveAndAck(t *testing.T) {
	st, srv := setup(t)
	add(t, st, 1, "orders", `{"id":1}`)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(srv.URL, "http") + "/v1/watch?token=secret"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()

	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(conn.Write(ctx, websocket.MessageText, []byte(`{"consumer":"bot","tables":["order.*"],"where":["id>=1"]}`)))
	read := func() map[string]any {
		_, b, err := conn.Read(ctx)
		must(err)
		var m map[string]any
		must(json.Unmarshal(b, &m))
		return m
	}
	if m := read(); m["type"] != "ready" {
		t.Fatalf("want ready, got %v", m)
	}
	if m := read(); m["type"] != "event" || m["event"].(map[string]any)["id"].(float64) != 1 {
		t.Fatalf("backlog: %v", m)
	}
	add(t, st, 2, "orders", `{"id":2}`) // live
	if m := read(); m["event"].(map[string]any)["id"].(float64) != 2 {
		t.Fatalf("live: %v", m)
	}
	must(conn.Write(ctx, websocket.MessageText, []byte(`{"type":"ack","id":2}`)))
	deadline := time.Now().Add(2 * time.Second)
	for {
		c, err := st.GetConsumer(ctx, "bot")
		if err == nil && c.Position == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("ack not saved: %+v %v", c, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Reconnecting as the consumer resumes after the ack.
	conn.Close(websocket.StatusNormalClosure, "")
	conn2, _, err := websocket.Dial(ctx, url, nil)
	must(err)
	defer conn2.CloseNow()
	must(conn2.Write(ctx, websocket.MessageText, []byte(`{"consumer":"bot"}`)))
	_, b, err := conn2.Read(ctx)
	must(err)
	if !strings.Contains(string(b), `"after":2`) {
		t.Fatalf("resume: %s", b)
	}
	// A bad subscription gets an error message, not a silent close.
	conn3, _, err := websocket.Dial(ctx, url, nil)
	must(err)
	defer conn3.CloseNow()
	must(conn3.Write(ctx, websocket.MessageText, []byte(`{"where":["nonsense"]}`)))
	_, b, _ = conn3.Read(ctx)
	if !strings.Contains(string(b), `"error":"bad_request"`) {
		t.Fatalf("bad subscription: %s", b)
	}
}

func TestSSE(t *testing.T) {
	st, srv := setup(t)
	add(t, st, 1, "t", `{"a":1}`)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", srv.URL+"/v1/stream?token=secret", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	buf := make([]byte, 4096)
	n, _ := res.Body.Read(buf)
	if got := string(buf[:n]); !strings.HasPrefix(got, "id: 1\nevent: change\ndata: {\"id\":1,") {
		t.Fatalf("sse: %q", got)
	}
}
