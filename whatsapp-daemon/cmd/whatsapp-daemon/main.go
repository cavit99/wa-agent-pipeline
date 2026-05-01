package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"golang.org/x/term"

	"openclaw/whatsapp-daemon/internal/auth"
	"openclaw/whatsapp-daemon/internal/backfill"
	"openclaw/whatsapp-daemon/internal/config"
	"openclaw/whatsapp-daemon/internal/control"
	"openclaw/whatsapp-daemon/internal/health"
	"openclaw/whatsapp-daemon/internal/listener"
	"openclaw/whatsapp-daemon/internal/paths"
	"openclaw/whatsapp-daemon/internal/writer"
)

func main() {
	code, err := run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	os.Exit(code)
}

func run() (int, error) {
	mode := "serve"
	args := os.Args[1:]
	if len(args) > 0 && args[0] != "" && args[0][0] != '-' {
		mode = args[0]
		args = args[1:]
	}
	if mode == "help" {
		printUsage(os.Stdout)
		return 0, nil
	}
	switch mode {
	case "pair", "serve", "backfill":
	default:
		return 1, fmt.Errorf("unknown mode %q; use pair, serve, or backfill", mode)
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return 1, err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if mode == "backfill" {
		return runBackfillClient(ctx, home, args)
	}
	return runDaemonMode(ctx, home, mode, args)
}

func runDaemonMode(ctx context.Context, home, mode string, args []string) (int, error) {
	root := paths.Root(home)
	fs := flag.NewFlagSet("whatsapp-daemon "+mode, flag.ContinueOnError)
	fs.Usage = func() {
		printUsage(fs.Output())
		fmt.Fprintf(fs.Output(), "\nOptions for %s:\n", mode)
		fs.PrintDefaults()
	}
	cfgPath := fs.String("config", config.DefaultPath(home), "allowlist config")
	dbPath := fs.String("db", paths.DefaultDB(root), "message-store sqlite db")
	mediaRoot := fs.String("media-root", filepath.Join(root, "media-cache"), "media cache root")
	authDB := fs.String("auth-db", filepath.Join(root, "whatsapp-daemon", "auth.db"), "whatsmeow auth sqlite db")
	logPath := fs.String("log", filepath.Join(root, "whatsapp-daemon", "daemon.log"), "daemon log path")
	healthPath := fs.String("health", filepath.Join(root, "whatsapp-daemon", "health.json"), "health status path")
	controlSocket := fs.String("control-socket", control.DefaultSocketPath(home), "daemon control socket path")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0, nil
		}
		return 1, err
	}

	log, err := health.NewLogger(*logPath)
	if err != nil {
		return 1, err
	}
	defer log.Close()
	status := health.NewStatus(*healthPath)
	stopHealth := make(chan struct{})
	go status.Run(stopHealth, time.Minute, log)
	defer close(stopHealth)

	cfg, err := config.New(*cfgPath)
	if err != nil {
		return 1, err
	}
	store, err := writer.New(*dbPath, *mediaRoot)
	if err != nil {
		return 1, err
	}
	defer store.Close()
	if err := store.EnsureGroups(context.Background(), cfg.Groups()); err != nil {
		return 1, err
	}
	h := &listener.Handler{Config: cfg, Store: store, Log: log, Status: status}
	opts := auth.Options{AuthDB: *authDB, Config: cfg, Handler: h, Health: status, Log: log}

	if mode == "serve" {
		opts.ServeReady = func(ctx context.Context, state auth.ServeState) (io.Closer, error) {
			srv := &control.Server{
				SocketPath: *controlSocket,
				Config:     cfg,
				Status:     status,
				Log:        log,
				Executor: &control.DaemonBackfillExecutor{
					DBPath:   *dbPath,
					Client:   state.Client,
					OnDemand: state.OnDemandProcessed,
					Log:      log,
				},
			}
			if err := srv.Start(ctx); err != nil {
				return nil, err
			}
			return srv, nil
		}
		startConfigReloaders(ctx, cfg, store, log)
	}

	switch mode {
	case "pair":
		return 0, auth.Pair(ctx, opts)
	case "serve":
		return 0, auth.Serve(ctx, opts)
	default:
		return 1, fmt.Errorf("unknown mode %q", mode)
	}
}

func startConfigReloaders(ctx context.Context, cfg *config.Manager, store *writer.Writer, log *health.Logger) {
	go cfg.Watch(ctx, 2*time.Second, func(err error) {
		log.Printf("allowlist_reload_failed error=%q", err.Error())
	})
	groupChanges := cfg.Subscribe()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-groupChanges:
				if err := store.EnsureGroups(ctx, cfg.Groups()); err != nil {
					log.Printf("group_migration_failed error=%q", err.Error())
				} else {
					log.Printf("allowlist_reloaded groups=%d", cfg.Count())
				}
			}
		}
	}()
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				if err := cfg.Reload(); err != nil {
					log.Printf("allowlist_reload_failed error=%q", err.Error())
				} else {
					_ = store.EnsureGroups(ctx, cfg.Groups())
					log.Printf("allowlist_reloaded groups=%d", cfg.Count())
				}
			}
		}
	}()
}

func runBackfillClient(ctx context.Context, home string, args []string) (int, error) {
	fs := flag.NewFlagSet("whatsapp-daemon backfill", flag.ContinueOnError)
	fs.Usage = func() {
		printUsage(fs.Output())
		fmt.Fprintln(fs.Output(), "\nOptions for backfill:")
		fs.PrintDefaults()
	}
	chatJID := fs.String("chat", "", "chat JID to backfill; must be allowlisted by the running daemon")
	tenant := fs.String("tenant", "", "optional tenant ownership check for this chat JID")
	beforeRaw := fs.String("before", "", "anchor before this timestamp: unix seconds, RFC3339, or YYYY-MM-DD")
	anchorMsgID := fs.String("anchor-msg-id", "", "optional exact local anchor message id")
	limit := fs.Int("limit", backfill.DefaultChunkSize, "maximum messages to request; capped at 50")
	controlSocket := fs.String("control-socket", control.DefaultSocketPath(home), "daemon control socket path")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0, nil
		}
		return 1, err
	}
	if *chatJID == "" {
		return 1, fmt.Errorf("backfill requires --chat <jid>")
	}
	before, err := backfill.ParseTimestamp(*beforeRaw)
	if err != nil {
		return 1, err
	}
	var beforeUnix int64
	if !before.IsZero() {
		beforeUnix = before.Unix()
	}
	return control.RunBackfillClient(ctx, control.ClientOptions{
		SocketPath: *controlSocket,
		Request: control.Request{
			ChatJID:     *chatJID,
			Tenant:      *tenant,
			AnchorMsgID: *anchorMsgID,
			BeforeUnix:  beforeUnix,
			Limit:       *limit,
		},
		Out:   os.Stdout,
		Human: term.IsTerminal(int(os.Stdout.Fd())),
	})
}

func printUsage(out io.Writer) {
	fmt.Fprintln(out, "Usage: whatsapp-daemon <pair|serve|backfill> [options]")
	fmt.Fprintln(out)
	fmt.Fprintln(out, "Subcommands:")
	fmt.Fprintln(out, "  pair      create or reuse whatsapp-daemon/auth.db and print a WhatsApp QR code when pairing is needed")
	fmt.Fprintln(out, "  serve     require an existing pair, listen for allowlisted group messages, write DB rows, media, health.json, and control.sock")
	fmt.Fprintln(out, "  backfill  ask the running serve daemon to request ON_DEMAND history sync over control.sock")
}
