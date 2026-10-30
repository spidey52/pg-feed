// pg-feed: a change stream for Postgres with a simple HTTP / SSE / WebSocket API.
//
//	pg-feed -config config.yaml          run
//	pg-feed -config config.yaml -check   check Postgres prerequisites and exit
//	pg-feed -version
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spidey52/pg-feed/internal/api"
	"github.com/spidey52/pg-feed/internal/config"
	"github.com/spidey52/pg-feed/internal/source"
	"github.com/spidey52/pg-feed/internal/store"
)

// Set at build time: -ldflags "-X main.version=1.2.3".
var version = "dev"

func main() {
	path := flag.String("config", "config.yaml", "path to the YAML config")
	check := flag.Bool("check", false, "check the Postgres prerequisites, print a report and exit")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return
	}

	cfg, err := config.Load(*path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	var level slog.Level
	_ = level.UnmarshalText([]byte(cfg.Log.Level))
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srcOpt := source.Options{
		URL: cfg.Source.URL, Slot: cfg.Source.Slot, Publication: cfg.Source.Publication,
		CreateIfMissing: cfg.Source.CreateIfMissing, Tables: cfg.Source.Tables,
	}

	if *check {
		report, err := source.New(srcOpt, nil, log).Inspect(ctx)
		if err != nil {
			fmt.Fprintln(os.Stderr, "check:", err)
			os.Exit(2)
		}
		out, _ := json.MarshalIndent(report, "", "  ")
		fmt.Println(string(out))
		if len(report.Problems) > 0 {
			os.Exit(1)
		}
		return
	}

	if err := os.MkdirAll(filepath.Dir(cfg.Store.Path), 0o755); err != nil {
		log.Error("store dir", "error", err)
		os.Exit(1)
	}
	st, err := store.Open(cfg.Store.Path)
	if err != nil {
		log.Error("open store", "error", err)
		os.Exit(1)
	}
	defer st.Close()

	src := source.New(srcOpt, st, log)
	if err := src.Setup(ctx); err != nil {
		log.Error("cannot start", "error", err)
		os.Exit(1)
	}

	apiOpt := api.Options{
		Tokens: cfg.Tokens(), CORSOrigins: cfg.Server.CORSOrigins, Version: version,
		Retention: api.Retention{Days: cfg.Store.Retention.Days, MaxSize: int64(cfg.Store.Retention.MaxSize)},
	}
	if len(apiOpt.Tokens) == 0 {
		log.Warn("server.tokens is empty: the API is open to anyone who can reach it")
	}
	srv := &http.Server{
		Addr: cfg.Server.Listen, Handler: api.New(st, src, apiOpt, log), ReadHeaderTimeout: 10 * time.Second,
		// Request contexts end with ctx, so open streams finish at shutdown.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	exitCode := 0

	go func() { // retention sweeper
		for {
			n, err := st.Sweep(ctx, apiOpt.Retention.Days, apiOpt.Retention.MaxSize)
			if err != nil {
				log.Warn("sweep", "error", err)
			} else if n > 0 {
				log.Info("sweep", "deleted", n)
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(cfg.Store.Retention.SweepEvery):
			}
		}
	}()
	go func() {
		log.Info("listening", "version", version, "addr", cfg.Server.Listen, "database", st.Database, "store", cfg.Store.Path)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "error", err)
			exitCode = 1
			stop()
		}
	}()

	_ = src.Run(ctx) // returns when ctx ends

	log.Info("stopping")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
	st.Close()
	os.Exit(exitCode)
}
