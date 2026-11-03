package main

// Embedded NAS tailnet node (Tier-2 T2.2): the manager joins its own
// embedded Headscale as a tsnet node so linked Macs reach the NAS from
// anywhere.
//
// Why tsnet rather than a separate tailscaled container: the Mac side
// already embeds tsnet (nfs/link.go), the dependency ships in this module,
// and no extra privileges (NET_ADMIN/TUN) are needed inside the container —
// tsnet runs in userspace networking mode.
//
// Flow, retried with capped backoff by the supervisor whenever it fails:
//
//  1. wait for the embedded Headscale to be running
//  2. mint a one-off preauth key against the local headscale (CLI), tagged
//     tag:jm-nas in service exposure (manager.LinkExposure)
//  3. join as node "juicemount-nas" over the LOCAL control address, so no
//     router hairpin NAT is needed to reach our own control server; in
//     service exposure, make sure the node carries tag:jm-nas (a node
//     registered before tagging keeps its identity and is tagged
//     server-side)
//  4. listen on the node's tailnet address for each JuiceMount service
//     (linkpolicy.go) and forward to its compose service
//  5. subnet exposure only: also advertise the LAN prefix and approve it
//     server-side
//  6. watch the node; if it leaves the tailnet, start over
//
// Off unless JM_NET_NAS_NODE=on; never takes the manager down on failure.

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"tailscale.com/ipn"
	"tailscale.com/tsnet"
)

const (
	defaultNASUDPPort = 41641
	nasNodeUpTimeout  = 60 * time.Second
	// nasNodeLostGrace is how long the node may report a logged-out state
	// (expired or deleted on the control server) before it is restarted with
	// a fresh key.
	nasNodeLostGrace = 30 * time.Second
)

func nasNodeEnabled() bool { return os.Getenv("JM_NET_NAS_NODE") == "on" }

// nasNodeHostname is this node's name in the tailnet directory.
func nasNodeHostname() string { return envOr("JM_NET_NAS_HOSTNAME", manager.DefaultNasHostname) }

// nasNodeUDPPort fixes the NAS node's WireGuard listener to the UDP port
// published by the container. Leaving tsnet on a random container-only port
// can strand peers on DERP: health checks stay quick, but multi-megabyte S3
// PUTs crawl until JuiceFS's request deadline aborts the mount. Operators may
// override the default when another tailnet service already owns 41641.
func nasNodeUDPPort() (uint16, error) {
	raw := strings.TrimSpace(envOr("JM_NET_NAS_UDP_PORT", strconv.Itoa(defaultNASUDPPort)))
	n, err := strconv.ParseUint(raw, 10, 16)
	if err != nil || n == 0 {
		return 0, fmt.Errorf("JM_NET_NAS_UDP_PORT must be an integer from 1 to 65535")
	}
	return uint16(n), nil
}

// nasNodeControlURL picks where the embedded node dials coordination. The
// node runs in the same process as Headscale, so it uses the in-container
// listener: dialing the public URL from inside the NAS needs router hairpin
// NAT, and without it the node never registers. Macs keep the public URL.
//
// With Headscale's own TLS on, the listener's certificate names the public
// host, so loopback cannot verify it. Then the public URL is used (needs
// hairpin NAT or split DNS) unless JM_NET_NAS_CONTROL_URL names an address
// that is both local and covered by the certificate.
func nasNodeControlURL() (string, error) {
	if override := strings.TrimRight(strings.TrimSpace(os.Getenv("JM_NET_NAS_CONTROL_URL")), "/"); override != "" {
		return override, nil
	}
	if os.Getenv("JM_NET_TLS_CERT") == "" && os.Getenv("JM_NET_TLS_KEY") == "" {
		_, port, err := net.SplitHostPort(hsListen)
		if err != nil {
			return "", err
		}
		return "http://" + net.JoinHostPort("127.0.0.1", port), nil
	}
	if url := externalURL(); url != "" {
		return url, nil
	}
	return "", fmt.Errorf("TLS configured but JM_NET_SERVER_URL unset: the embedded node cannot verify a self-referenced certificate; set JM_NET_SERVER_URL=https://<cert-name>:30193")
}

// nasNodeConfig is one NAS node start, resolved from the environment in
// production and built directly by tests.
type nasNodeConfig struct {
	ControlURL string
	Hostname   string
	Dir        string
	UDPPort    uint16
	Services   []nasService
	Subnet     string // subnet exposure's LAN route; "" in service exposure
	// Tags are the tags MintKey's key carries and EnsureTags applies.
	Tags    []string
	MintKey func() (string, error)
	// EnsureTags tags the registered node server-side when its key did not
	// (an identity from before keys were tagged) and reports whether it did.
	// nil skips it.
	EnsureTags func() (bool, error)
	// ApproveRoute approves Subnet on the control server (subnet exposure
	// only).
	ApproveRoute func(prefix string) error
	Dial         nasTCPDialer
	UpTimeout    time.Duration
	HealthEvery  time.Duration
	Logf         func(string, ...any)
}

// nasNodeConfigFromEnv resolves one NAS node start. cfgPath is Headscale's
// config.yaml; the node's state lives next to it, where manager.LinkExposure
// looks for it.
func nasNodeConfigFromEnv(cfgPath string) (nasNodeConfig, error) {
	control, err := nasNodeControlURL()
	if err != nil {
		return nasNodeConfig{}, err
	}
	udpPort, err := nasNodeUDPPort()
	if err != nil {
		return nasNodeConfig{}, err
	}
	services, err := nasServices()
	if err != nil {
		return nasNodeConfig{}, err
	}
	exposure, subnet, err := manager.LinkExposure(cfgPath)
	if err != nil {
		return nasNodeConfig{}, err
	}
	if subnet != "" {
		if _, err := netip.ParsePrefix(subnet); err != nil {
			return nasNodeConfig{}, fmt.Errorf("bad JM_NET_NAS_ROUTE %q: %w", subnet, err)
		}
	}
	var tags []string
	hostname := nasNodeHostname()
	var ensureTags func() (bool, error)
	if exposure == manager.LinkExposureServices {
		// A Mac finds a NAS node that serves its ports by this tag
		// (nfs linkNASServicePeer), so it is applied under every policy:
		// Headscale honors a tagged key's tags with or without one. A
		// subnet-exposure node stays untagged, so a Mac waits for the route
		// instead of dialing the node's tailnet address before the route
		// converges.
		tags = []string{manager.LinkTagNAS}
		ensureTags = func() (bool, error) {
			changed, err := manager.EnsureNodeTags(hsBinary, cfgPath, hostname, tags)
			if err != nil {
				return false, fmt.Errorf("%w (Headscale tags a node registered before tagged keys only when its ACL policy defines the tag: use the Manager's policy instead of JM_NET_POLICY=off, add %q: [] to your own policy's tagOwners, or set JM_NET_NAS_EXPOSURE=subnet)", err, manager.LinkTagNAS)
			}
			return changed, nil
		}
	}
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return nasNodeConfig{
		ControlURL: control,
		Hostname:   hostname,
		Dir:        filepath.Join(filepath.Dir(cfgPath), "nas-node"),
		UDPPort:    udpPort,
		Services:   services,
		Subnet:     subnet,
		Tags:       tags,
		EnsureTags: ensureTags,
		MintKey: func() (string, error) {
			return manager.MintPreauthKey(hsBinary, cfgPath, false, tags)
		},
		ApproveRoute: func(prefix string) error {
			return manager.ApproveSubnetRoutes(hsBinary, cfgPath, hostname, []string{prefix})
		},
		Dial:        dialer.DialContext,
		UpTimeout:   nasNodeUpTimeout,
		HealthEvery: 10 * time.Second,
		Logf:        func(string, ...any) {},
	}, nil
}

// newNASNodeSupervisor supervises the NAS node. It waits for control to be
// running before each attempt, so a slow Headscale start is not counted as a
// NAS node failure.
func newNASNodeSupervisor(cfgPath string, control *headscaleSupervisor) *supervisor {
	var sup *supervisor
	sup = newSupervisor("nas node", func(stop <-chan struct{}, ready func(string)) error {
		for control != nil && !control.status.running() {
			sup.status.set("starting", "waiting for headscale")
			select {
			case <-stop:
				return errStopped
			case <-time.After(500 * time.Millisecond):
			}
		}
		cfg, err := nasNodeConfigFromEnv(cfgPath)
		if err != nil {
			return err
		}
		if !strings.HasPrefix(cfg.ControlURL, "http://127.0.0.1:") {
			log.Printf("JuiceMount Link: nas node dials control at %s; without hairpin NAT or split DNS for that name it cannot register", cfg.ControlURL)
		}
		return runNASNode(cfg, stop, ready)
	})
	return sup
}

// runNASNode is one NAS node lifetime. It returns errStopped after stop
// closes, or the reason the node failed or left the tailnet.
func runNASNode(cfg nasNodeConfig, stop <-chan struct{}, ready func(string)) error {
	key, err := cfg.MintKey()
	if err != nil {
		return fmt.Errorf("mint nas preauth key: %w", err)
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	// Tags come from the key (or EnsureTags), not AdvertiseTags: Headscale
	// 0.29 refuses a tagged key's registration when the node also requests
	// tags.
	s := &tsnet.Server{
		Hostname:   cfg.Hostname,
		ControlURL: cfg.ControlURL,
		AuthKey:    key,
		Dir:        cfg.Dir,
		Ephemeral:  false, // persistent node: stable tailnet IP across restarts
		Port:       cfg.UDPPort,
		Logf:       logf,
	}
	if err := s.Start(); err != nil {
		return fmt.Errorf("nas node start: %w", err)
	}
	defer s.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()
	upCtx, upCancel := context.WithTimeout(ctx, cfg.UpTimeout)
	st, err := s.Up(upCtx)
	upCancel()
	if err != nil {
		if ctx.Err() != nil {
			return errStopped
		}
		return fmt.Errorf("nas node up via %s: %w", cfg.ControlURL, err)
	}
	if cfg.EnsureTags != nil {
		changed, err := cfg.EnsureTags()
		if err != nil {
			return fmt.Errorf("nas node tags: %w", err)
		}
		if changed {
			// Headscale does not push the retagged node a new packet filter
			// on its live session; the policy's rules for tag:jm-nas reach
			// it only on the next connection. Rejoin with the same identity.
			return fmt.Errorf("nas node tagged %s; rejoining to load its policy", cfg.Hostname)
		}
	}

	ports := make([]string, 0, len(cfg.Services))
	for _, svc := range cfg.Services {
		ln, err := s.Listen("tcp", ":"+strconv.Itoa(int(svc.Port)))
		if err != nil {
			return fmt.Errorf("nas node listen on tailnet port %d: %w", svc.Port, err)
		}
		defer ln.Close()
		go serveNASService(ln, svc.Target, cfg.Dial)
		ports = append(ports, strconv.Itoa(int(svc.Port)))
	}
	addr := ""
	if len(st.TailscaleIPs) > 0 {
		addr = st.TailscaleIPs[0].String()
	}
	detail := fmt.Sprintf("%q at %s serving ports %s", cfg.Hostname, addr, strings.Join(ports, ","))

	if cfg.Subnet != "" {
		prefix, err := netip.ParsePrefix(cfg.Subnet)
		if err != nil {
			return fmt.Errorf("bad JM_NET_NAS_ROUTE %q: %w", cfg.Subnet, err)
		}
		lc, err := s.LocalClient()
		if err != nil {
			return fmt.Errorf("nas node local client: %w", err)
		}
		if _, err := lc.EditPrefs(ctx, &ipn.MaskedPrefs{
			Prefs:              ipn.Prefs{AdvertiseRoutes: []netip.Prefix{prefix}},
			AdvertiseRoutesSet: true,
		}); err != nil {
			return fmt.Errorf("nas node advertise %s: %w", prefix, err)
		}
		// tsnet's fake-TUN mode deliberately rejects routed TCP flows that do
		// not have a listener or fallback handler. Advertising and approving a
		// subnet alone therefore creates a route that looks healthy but returns
		// RST for Redis, object storage, and every other LAN service. Install the
		// system-network forwarder before the route can be approved.
		deregister := s.RegisterFallbackTCPHandler(nasRouteTCPHandler(prefix, cfg.Dial))
		defer deregister()
		if cfg.ApproveRoute != nil {
			go approveLoop(ctx, cfg.ApproveRoute, prefix.String(), cfg.Hostname)
		}
		detail += ", subnet route " + prefix.String()
	}
	ready(detail)

	lc, err := s.LocalClient()
	if err != nil {
		return fmt.Errorf("nas node local client: %w", err)
	}
	every := cfg.HealthEvery
	if every <= 0 {
		every = 10 * time.Second
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	var lostSince time.Time
	for {
		select {
		case <-stop:
			return errStopped
		case <-ticker.C:
		}
		statusCtx, statusCancel := context.WithTimeout(ctx, 5*time.Second)
		cur, err := lc.StatusWithoutPeers(statusCtx)
		statusCancel()
		if err != nil {
			continue
		}
		switch cur.BackendState {
		case ipn.NeedsLogin.String(), ipn.NeedsMachineAuth.String(), ipn.Stopped.String():
			if lostSince.IsZero() {
				lostSince = time.Now()
			}
			if time.Since(lostSince) >= nasNodeLostGrace {
				return fmt.Errorf("nas node left the tailnet (state %s)", cur.BackendState)
			}
		default:
			lostSince = time.Time{}
		}
	}
}

type nasTCPDialer func(context.Context, string, string) (net.Conn, error)

// serveNASService forwards every connection accepted on a tailnet listener
// to target. The listener only exists for the configured JuiceMount ports;
// the ACL policy decides which peers may connect at all.
func serveNASService(ln net.Listener, target string, dial nasTCPDialer) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go forwardNASTCP(conn, target, dial)
	}
}

// nasRouteTCPHandler forwards only addresses inside the explicitly
// advertised NAS route. Returning intercept=false for everything else keeps
// the tsnet node from becoming an accidental exit node or generic proxy.
func nasRouteTCPHandler(prefix netip.Prefix, dial nasTCPDialer) tsnet.FallbackTCPHandler {
	return func(_ netip.AddrPort, dst netip.AddrPort) (func(net.Conn), bool) {
		if !prefix.Contains(dst.Addr()) {
			return nil, false
		}
		return func(client net.Conn) {
			forwardNASTCP(client, dst.String(), dial)
		}, true
	}
}

func forwardNASTCP(client net.Conn, target string, dial nasTCPDialer) {
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	upstream, err := dial(ctx, "tcp", target)
	cancel()
	if err != nil {
		log.Printf("JuiceMount Link: TCP dial to %s failed: %v", target, err)
		return
	}
	defer upstream.Close()

	done := make(chan struct{}, 2)
	copyHalf := func(dstConn, srcConn net.Conn) {
		_, _ = io.Copy(dstConn, srcConn)
		if closeWriter, ok := dstConn.(interface{ CloseWrite() error }); ok {
			_ = closeWriter.CloseWrite()
		}
		done <- struct{}{}
	}
	go copyHalf(upstream, client)
	go copyHalf(client, upstream)
	<-done
	<-done
}

// approveLoop approves the advertised route server-side until it sticks.
// The route only appears in headscale's directory once the node's
// registration fully propagates, hence the retries (~2 min ceiling).
func approveLoop(ctx context.Context, approve func(string) error, prefix, host string) {
	for tries := 0; tries < 24; tries++ {
		if err := approve(prefix); err == nil {
			log.Printf("JuiceMount Link: subnet route %s approved for %q", prefix, host)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	log.Printf("JuiceMount Link: WARNING could not approve subnet route %s (linked Macs will not reach LAN services); retry via POST /api/net/routes/approve", prefix)
}
