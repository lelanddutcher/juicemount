// Command jm-respproxy runs the H-8 option-B RESP proxy in front of the
// JuiceFS metadata Redis: it authenticates every client as a named principal,
// refuses admin-only and unknown write shapes, and hands project-scoped
// writes to the lease policy. See internal/respproxy.
//
//	jm-respproxy -config /etc/jm-respproxy/config.json
//	jm-respproxy hash            # reads a password on stdin, prints an argon2id PHC hash
//
// The upstream Redis password is read from the environment variable named by
// "upstream_password_env" (default JM_RESPPROXY_UPSTREAM_PASSWORD), never
// from argv or the config file. SIGHUP reloads the principals file.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lelanddutcher/juicemount/internal/auditlog"
	"github.com/lelanddutcher/juicemount/internal/respproxy"
)

type fileConfig struct {
	Listen              string `json:"listen"`
	Upstream            string `json:"upstream"`
	UpstreamUser        string `json:"upstream_user"`
	UpstreamPasswordEnv string `json:"upstream_password_env"`
	Stats               string `json:"stats"`
	Principals          string `json:"principals"`
	Mode                string `json:"mode"`
	VolumeDB            *int   `json:"volume_db"`
	AllowedDBs          []int  `json:"allowed_dbs"`
	Singleton           *bool  `json:"singleton"`
	TrackAnchors        bool   `json:"track_anchors"`
	StageBirth          bool   `json:"stage_birth"`
	ReloadHandshake     bool   `json:"reload_handshake"`
	// AuditDir holds the per-person mutation log (internal/auditlog);
	// empty = no log. The entrypoint sets <principals dir>/audit, which
	// the Manager reads; the writer's health file is AuditDir + ".health".
	AuditDir string `json:"audit_dir"`
	// Audit: "auto" (or empty) opens the log lazily: nothing is written
	// until the principals table names a person (or grants are in force).
	// "on" logs from the start.
	Audit string `json:"audit"`
	// AuditRetainMB / AuditRetainDays bound the log (oldest segments are
	// removed); 0 = the auditlog defaults, -1 = no limit.
	AuditRetainMB   int64 `json:"audit_retain_mb"`
	AuditRetainDays int64 `json:"audit_retain_days"`
	// SmartFolderRoots names the Manager's smart-folder roots in the
	// volume root: the manager principal may remove an empty view folder
	// it created directly under one (internal/respproxy smartview.go).
	// Absent: respproxy.DefaultSmartRoots; [] = none. They must match the
	// Manager's JM_SMART_FOLDERS_ROOT / _COLLECTIONS_ROOT / _QUERIES_ROOT;
	// the entrypoint renders them from those variables.
	SmartFolderRoots *[]string `json:"smart_folder_roots"`
}

// smartRoots resolves smart_folder_roots; invalid names are left out and
// reported.
func (fc fileConfig) smartRoots() ([]string, []error) {
	if fc.SmartFolderRoots == nil {
		return respproxy.CleanSmartRoots(respproxy.DefaultSmartRoots)
	}
	return respproxy.CleanSmartRoots(*fc.SmartFolderRoots)
}

// auditOptions maps the config to the audit writer's options.
func (fc fileConfig) auditOptions() (auditlog.Options, error) {
	o := auditlog.Options{HealthPath: fc.AuditDir + ".health"}
	switch fc.Audit {
	case "", "auto":
		o.Lazy = true
	case "on":
	default:
		return o, fmt.Errorf("audit must be \"auto\" or \"on\", not %q", fc.Audit)
	}
	switch {
	case fc.AuditRetainMB < -1 || fc.AuditRetainDays < -1:
		return o, fmt.Errorf("audit_retain_mb and audit_retain_days must be -1, 0 or positive")
	case fc.AuditRetainMB == -1:
		o.RetainBytes = -1
	default:
		o.RetainBytes = fc.AuditRetainMB << 20
	}
	if fc.AuditRetainDays == -1 {
		o.RetainAge = -1
	} else {
		o.RetainAge = time.Duration(fc.AuditRetainDays) * 24 * time.Hour
	}
	return o, nil
}

// Resolve the same effective defaults used by the CLI before constructing a
// proxy. New validates birth prerequisites for this and every other caller.
func (fc fileConfig) proxyConfig() (respproxy.Config, error) {
	m, ok := respproxy.ParseMode(fc.Mode)
	if !ok {
		return respproxy.Config{}, fmt.Errorf("bad mode %q", fc.Mode)
	}
	volDB, singleton := 1, true
	if fc.VolumeDB != nil {
		volDB = *fc.VolumeDB
	}
	if fc.Singleton != nil {
		singleton = *fc.Singleton
	}
	if fc.ReloadHandshake && !singleton {
		return respproxy.Config{}, fmt.Errorf("reload_handshake requires singleton ownership")
	}
	return respproxy.Config{Upstream: fc.Upstream, UpstreamUser: fc.UpstreamUser,
		Mode: m, VolumeDB: volDB, AllowedDBs: fc.AllowedDBs,
		Singleton: singleton, StageBirth: fc.StageBirth}, nil
}

func main() {
	if len(os.Args) > 1 && os.Args[1] == "hash" {
		os.Exit(hashCmd())
	}
	cfgPath := flag.String("config", "/etc/jm-respproxy/config.json", "config file (JSON)")
	listen := flag.String("listen", "", "override: address clients connect to")
	upstream := flag.String("upstream", "", "override: Redis host:port")
	mode := flag.String("mode", "", "override: passthrough | observe | enforce")
	flag.Parse()

	fc := fileConfig{
		Listen: "127.0.0.1:30179", Upstream: "127.0.0.1:6379", Stats: "127.0.0.1:30180",
		Principals: "/etc/jm-respproxy/principals.json", Mode: "enforce",
		UpstreamPasswordEnv: "JM_RESPPROXY_UPSTREAM_PASSWORD",
	}
	if b, err := os.ReadFile(*cfgPath); err == nil {
		dec := json.NewDecoder(strings.NewReader(string(b)))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&fc); err != nil {
			log.Fatalf("config %s: %v", *cfgPath, err)
		}
	} else if *cfgPath != "/etc/jm-respproxy/config.json" || !os.IsNotExist(err) {
		log.Fatalf("config %s: %v", *cfgPath, err)
	}
	if *listen != "" {
		fc.Listen = *listen
	}
	if *upstream != "" {
		fc.Upstream = *upstream
	}
	if *mode != "" {
		fc.Mode = *mode
	}
	proxyCfg, err := fc.proxyConfig()
	if err != nil {
		log.Fatal(err)
	}
	ps, err := respproxy.LoadPrincipals(fc.Principals)
	if err != nil {
		log.Fatalf("principals: %v", err)
	}
	var leaseMu sync.Mutex
	var leases *respproxy.LeaseEngine
	proxyCfg.BeforeSideClose = func() {
		leaseMu.Lock()
		defer leaseMu.Unlock()
		if leases != nil {
			leases.Close()
		}
	}
	proxyCfg.Principals = ps
	proxyCfg.UpstreamPassword = os.Getenv(fc.UpstreamPasswordEnv)
	var audit *auditlog.Writer
	if fc.AuditDir != "" {
		// A log that cannot open does not stop the proxy: the writer starts
		// failed and retries, and Mac writes are refused only while grants
		// are in force (fail closed); otherwise they go ahead unlogged.
		opts, err := fc.auditOptions()
		if err != nil {
			log.Fatalf("config %s: %v", *cfgPath, err)
		}
		audit, err = auditlog.Open(fc.AuditDir, opts)
		if err != nil {
			log.Printf("audit log %s unavailable (retrying): %v", fc.AuditDir, err)
		}
		defer audit.Close() // after the proxy joined: every outcome is queued
		proxyCfg.Audit = audit
	}
	p, err := respproxy.New(proxyCfg)
	if err != nil {
		log.Fatal(err)
	}
	p.Stats.TrackAnchors = fc.TrackAnchors
	closeJoined := func() { _ = p.Close(); <-p.Done() }
	if err := p.Start(); err != nil {
		closeJoined()
		log.Fatal(err)
	}
	var reloader *respproxy.PrincipalReloader
	if fc.ReloadHandshake {
		reloader, err = respproxy.NewPrincipalReloader(fc.Principals, p.Auth)
		if err != nil {
			closeJoined()
			log.Fatalf("principals handshake startup: %v", err)
		}
	}
	// Lane L2: the lease engine is the Policy and serves JM.LEASE.
	// Publish the owned engine atomically with respect to asynchronous
	// singleton-loss shutdown. Never start it before acquiring ownership.
	leaseMu.Lock()
	if p.Closing() {
		leaseMu.Unlock()
		closeJoined()
		log.Fatal("proxy closed before lease engine startup")
	}
	smartRoots, badRoots := fc.smartRoots()
	for _, err := range badRoots {
		log.Printf("smart_folder_roots: left out: %v", err)
	}
	leases = respproxy.NewLeaseEngine(respproxy.LeaseConfig{Side: p.Side(), VolumeDB: proxyCfg.VolumeDB, SmartRoots: smartRoots})
	err = leases.Start()
	if err == nil {
		p.SetPolicy(leases)
	}
	leaseMu.Unlock()
	if err != nil || p.Closing() {
		closeJoined()
		log.Fatalf("lease engine startup interrupted: %v", err)
	}
	ln, err := net.Listen("tcp", fc.Listen)
	if err != nil {
		closeJoined()
		log.Fatalf("listen: %v", err)
	}
	if fc.Stats != "" {
		go func() {
			if err := http.ListenAndServe(fc.Stats, p.Stats.Handler()); err != nil {
				log.Printf("stats: %v", err)
			}
		}()
	}
	ch := make(chan os.Signal, 4)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(ch)
	controlDone := make(chan struct{})
	go func() {
		defer close(controlDone)
		var ticks <-chan time.Time
		reloadTick := func() {
			if !p.Closing() {
				if err := reloader.Tick(); err != nil {
					log.Printf("principals handshake pending: %v", err)
				}
			}
		}
		if reloader != nil {
			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			ticks = ticker.C
			reloadTick()
		}
		for {
			var s os.Signal
			select {
			case <-p.Done():
				return
			case <-ticks:
				reloadTick()
				continue
			case s = <-ch:
			}
			if s == syscall.SIGHUP {
				if reloader != nil {
					reloadTick()
					continue
				}
				nps, err := respproxy.LoadPrincipals(fc.Principals)
				if err != nil {
					log.Printf("principals reload refused (keeping the old table): %v", err)
					continue
				}
				p.Auth.Swap(nps)
				log.Printf("principals reloaded: %d enabled", nps.Len())
				continue
			}
			p.Close()
			return
		}
	}()
	log.Printf("jm-respproxy: %s -> %s mode=%s principals=%d singleton=%v incarnation=%s smart_folder_roots=%q",
		fc.Listen, fc.Upstream, p.Mode(), ps.Len(), proxyCfg.Singleton, p.Incarnation(), smartRoots)
	err = p.Serve(ln)
	closeJoined()
	<-controlDone
	if audit != nil {
		audit.Close() // log.Fatalf below skips deferred calls
	}
	if err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func hashCmd() int {
	fs := flag.NewFlagSet("hash", flag.ExitOnError)
	t := fs.Uint("t", respproxy.DefaultArgonTime, "argon2id time cost")
	mKiB := fs.Uint("m", respproxy.DefaultArgonMemory, "argon2id memory (KiB)")
	fs.Parse(os.Args[2:])
	pw, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && pw == "" {
		fmt.Fprintln(os.Stderr, "hash: read password from stdin:", err)
		return 1
	}
	pw = strings.TrimRight(pw, "\r\n")
	if pw == "" {
		fmt.Fprintln(os.Stderr, "hash: empty password")
		return 1
	}
	h, err := respproxy.HashPassword(pw, uint32(*t), uint32(*mKiB), respproxy.DefaultArgonThreads)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hash:", err)
		return 1
	}
	fmt.Println(h)
	return 0
}
