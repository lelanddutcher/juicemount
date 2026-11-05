// juicemount-mcp is the MCP server through which an AI agent on a Mac
// (Claude Desktop, Claude Code and others) finds files on the JuiceMount
// volume and edits collections through the Manager, as the Teams person who
// minted its agent token (internal/agentmcp).
//
// Usage:
//
//	juicemount-mcp serve [--mount /Volumes/zpool]
//	juicemount-mcp token create --name <name> [--days N] [--print]
//	juicemount-mcp token list
//	juicemount-mcp token revoke <id> [--local-only]
//	juicemount-mcp token import --manager <url> < token.txt
//
// serve speaks MCP over stdin and stdout (stdout carries protocol messages
// only; logs go to stderr). The mount point is --mount, else
// $JUICEMOUNT_MOUNT, else /Volumes/zpool (as the juicemount CLI).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/agentmcp"
	"github.com/lelanddutcher/juicemount/internal/credstore"
	"github.com/lelanddutcher/juicemount/internal/deviceid"
	"github.com/lelanddutcher/juicemount/internal/version"
)

const defaultMount = "/Volumes/zpool"

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

const usage = `juicemount-mcp - the JuiceMount MCP server for AI agents

Commands:
  serve [--mount DIR]                      Run the MCP server on stdin/stdout
  token create --name NAME [--days N]      Mint an agent token from this Mac and store it
               [--print]                   ... or print it (stdout) instead of storing it
  token list                               List your agent tokens
  token revoke ID [--local-only]           Revoke a token and delete it here
  token import --manager URL               Store a token read from stdin
`

func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch args[0] {
	case "serve":
		err = serve(ctx, args[1:], stdin, stdout, stderr)
	case "token":
		err = token(ctx, args[1:], stdin, stdout, stderr)
	case "help", "-h", "--help":
		fmt.Fprint(stdout, usage)
		return 0
	default:
		fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	if errors.Is(err, flag.ErrHelp) {
		return 2
	}
	if err != nil {
		fmt.Fprintln(stderr, "juicemount-mcp:", err)
		return 1
	}
	return 0
}

func serve(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(stderr)
	mount := fs.String("mount", "", "the volume's mount point on this Mac (default $JUICEMOUNT_MOUNT, else "+defaultMount+")")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("serve takes no arguments")
	}
	if *mount == "" {
		*mount = os.Getenv("JUICEMOUNT_MOUNT")
	}
	if *mount == "" {
		*mount = defaultMount
	}
	root, err := agentmcp.CleanMount(*mount)
	if err != nil {
		return err
	}
	logger := log.New(stderr, "juicemount-mcp: ", log.LstdFlags)
	store := keychainStore{}
	hc := agentmcp.NewHTTPClient()
	tools := &agentmcp.Tools{Mount: root, Log: logger, Sleep: sleepCtx(ctx), Client: func() (*agentmcp.Client, error) {
		// Read on every call: a token created or revoked while the agent
		// runs takes effect without a restart.
		rec, err := store.Load()
		if err != nil {
			return nil, err
		}
		return agentmcp.NewAgentClient(rec.ManagerURL, rec.Token, hc), nil
	}}
	if rec, err := store.Load(); err != nil {
		logger.Printf("no usable agent token yet (%v); tool calls fail until one is created", err)
	} else {
		logger.Printf("serving with agent token %s for %s; mount point %s", rec.ID, rec.ManagerURL, root)
		if w := agentmcp.CleartextWarning(rec.ManagerURL); w != "" {
			logger.Print(w)
		}
	}
	srv := &agentmcp.Server{Tools: tools, Log: logger, Version: version.Version}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, stdin, stdout) }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		logger.Printf("signalled; shutting down")
		return nil
	}
}

// sleepCtx waits d, or until ctx ends.
func sleepCtx(ctx context.Context) func(time.Duration) {
	return func(d time.Duration) {
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
		case <-ctx.Done():
		}
	}
}

func token(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return flag.ErrHelp
	}
	// A person runs these in a terminal: the Keychain may ask them (after a
	// rebuild or update, before this build may use the stored token).
	credstore.AllowAgentTokenUI()
	tc := &agentmcp.TokenCommands{Store: keychainStore{}, Mac: macCredentials, HTTP: agentmcp.NewHTTPClient(), Out: stdout, Err: stderr}
	fs := flag.NewFlagSet("token "+args[0], flag.ContinueOnError)
	fs.SetOutput(stderr)
	switch args[0] {
	case "create":
		name := fs.String("name", "", "a name for the token, e.g. the agent that will use it")
		days := fs.Int("days", 0, "days until it expires, 1-90 (default 90)")
		print := fs.Bool("print", false, "write the token to stdout instead of storing it (to import it elsewhere)")
		if err := parse(fs, args[1:], 0); err != nil {
			return err
		}
		return tc.Create(ctx, *name, *days, *print)
	case "list":
		if err := parse(fs, args[1:], 0); err != nil {
			return err
		}
		return tc.List(ctx)
	case "revoke":
		localOnly := fs.Bool("local-only", false, "only delete the token from this machine; do not ask the Manager")
		// The ID may come before or after the flag.
		rest := args[1:]
		id := ""
		if len(rest) > 0 && len(rest[0]) > 0 && rest[0][0] != '-' {
			id, rest = rest[0], rest[1:]
		}
		if err := fs.Parse(rest); err != nil {
			return err
		}
		if id == "" && fs.NArg() == 1 {
			id = fs.Arg(0)
		} else if fs.NArg() > 0 || id == "" {
			return fmt.Errorf("usage: juicemount-mcp token revoke <id> [--local-only]")
		}
		return tc.Revoke(ctx, id, *localOnly)
	case "import":
		manager := fs.String("manager", "", "the Manager's address, e.g. http://nas.local:30190")
		if err := parse(fs, args[1:], 0); err != nil {
			return err
		}
		if *manager == "" {
			return fmt.Errorf("--manager is required")
		}
		return tc.Import(ctx, stdin, *manager)
	}
	return fmt.Errorf("unknown token command %q", args[0])
}

func parse(fs *flag.FlagSet, args []string, nargs int) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != nargs {
		return fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	return nil
}

// macCredentials reads this Mac's enrollment from the app's Keychain item
// (macOS may ask to allow juicemount-mcp to read it).
func macCredentials() (agentmcp.MacCredentials, error) {
	raw, err := credstore.LoadEnrollment()
	if errors.Is(err, credstore.ErrNotFound) {
		return agentmcp.MacCredentials{}, errors.New("this machine has no JuiceMount Manager enrollment: run this on a Mac whose JuiceMount is approved by the Manager")
	}
	if err != nil {
		return agentmcp.MacCredentials{}, fmt.Errorf("reading this Mac's enrollment: %v", err)
	}
	device, _ := deviceid.Fingerprint()
	return agentmcp.MacCredentialsFromState(raw, device)
}

// keychainStore keeps the token record with internal/credstore.
type keychainStore struct{}

func (keychainStore) Save(r agentmcp.TokenRecord) error { return credstore.SaveAgentToken(r.Encode()) }

func (keychainStore) Load() (agentmcp.TokenRecord, error) {
	raw, err := credstore.LoadAgentToken()
	if errors.Is(err, credstore.ErrNotFound) {
		return agentmcp.TokenRecord{}, agentmcp.ErrNoToken
	}
	if err != nil {
		return agentmcp.TokenRecord{}, err
	}
	return agentmcp.ParseTokenRecord(raw)
}

func (keychainStore) Delete() error { return credstore.DeleteAgentToken() }
