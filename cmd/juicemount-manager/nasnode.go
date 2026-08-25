package main

// Embedded NAS tailnet node (Tier-2 T2.2): the manager joins its own
// embedded Headscale as a tsnet node and advertises the NAS LAN subnet, so
// linked Macs can reach Redis/MinIO/manager over the tailnet from anywhere.
//
// Why tsnet rather than a separate tailscaled container: the Mac side
// already embeds tsnet (nfs/link.go), the dependency ships in this module,
// and no extra privileges (NET_ADMIN/TUN) are needed inside the container —
// tsnet runs in userspace networking mode.
//
// Flow, matching headscale's two-step routing model:
//
//  1. mint a one-off preauth key against the local headscale (CLI)
//  2. join as node "juicemount-nas" (ControlURL = external server_url, or
//     the in-container loopback listener when no external URL is set)
//  3. advertise JM_NET_NAS_ROUTE (default 192.168.0.0/24) via prefs
//  4. approve the route server-side (`nodes approve-routes`), retrying
//     until the registration propagates
//
// Off unless JM_NET_NAS_NODE=on; never takes the manager down on failure.

import (
	"context"
	"fmt"
	"log"
	"net/netip"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/lelanddutcher/juicemount/internal/manager"
	"tailscale.com/ipn"
	"tailscale.com/tsnet"
)

const (
	nasNodeDir = "/data/headscale/nas-node"
	nasUser    = "jm"
)

func nasNodeEnabled() bool { return os.Getenv("JM_NET_NAS_NODE") == "on" }

// nasRoutePrefix is the subnet advertised to the tailnet.
func nasRoutePrefix() string { return envOr("JM_NET_NAS_ROUTE", manager.NasLanSubnet) }

// nasNodeHostname is this node's name in the tailnet directory.
func nasNodeHostname() string { return envOr("JM_NET_NAS_HOSTNAME", manager.DefaultNasHostname) }

// nasNodeControlURL picks where the embedded node dials coordination:
// the externally-visible server_url when set (required once TLS is on, so
// certificate verification sees the operator's real name), otherwise the
// in-container headscale listener directly.
func nasNodeControlURL() (string, error) {
	if url := externalURL(); url != "" {
		return url, nil
	}
	if os.Getenv("JM_NET_TLS_CERT") != "" || os.Getenv("JM_NET_TLS_KEY") != "" {
		return "", fmt.Errorf("TLS configured but JM_NET_SERVER_URL unset: the embedded node cannot verify a self-referenced certificate; set JM_NET_SERVER_URL=https://<cert-name>:30193")
	}
	return "http://127.0.0.1:" + strings.SplitN(hsListen, ":", 2)[1], nil
}

// lastKeyLine extracts the preauth key from CLI output: the last non-empty
// line that is not a timestamped log line (same contract as the manager's
// parser).
func lastKeyLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" && !strings.HasPrefix(line, "20") {
			return line
		}
	}
	return ""
}

// mintNasPreauthKey creates a one-off key for the NAS node itself. Retried:
// right after headscale boots, its gRPC socket may not accept commands yet.
func mintNasPreauthKey(cfgPath string) (string, error) {
	var lastErr error
	for tries := 0; tries < 20; tries++ {
		args := append([]string{"--config", cfgPath}, manager.PreauthKeyArgs(false)...)
		args = append(args, "--expiration", "1h")
		out, err := exec.Command(hsBinary, args...).CombinedOutput()
		if err == nil {
			if key := lastKeyLine(string(out)); key != "" {
				return key, nil
			}
			lastErr = fmt.Errorf("empty preauth key output")
		} else {
			lastErr = fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
		}
		time.Sleep(500 * time.Millisecond)
	}
	return "", fmt.Errorf("mint nas preauth key: %w", lastErr)
}

// nasNodeSupervisor owns the embedded node's lifecycle.
type nasNodeSupervisor struct {
	srv *tsnet.Server
}

// Start brings the node up, advertises the LAN route, and kicks off the
// server-side approval loop. Non-fatal on error (Link degrades, manager
// stays up).
func (n *nasNodeSupervisor) Start(cfgPath string) error {
	control, err := nasNodeControlURL()
	if err != nil {
		return err
	}
	key, err := mintNasPreauthKey(cfgPath)
	if err != nil {
		return err
	}
	s := &tsnet.Server{
		Hostname:   nasNodeHostname(),
		ControlURL: control,
		AuthKey:    key,
		Dir:        nasNodeDir,
		Ephemeral:  false, // persistent node: stable tailnet IP across restarts
		Logf:       func(string, ...any) {},
	}
	if err := s.Start(); err != nil {
		return fmt.Errorf("nas node start: %w", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := s.Up(ctx); err != nil {
		s.Close()
		return fmt.Errorf("nas node up: %w", err)
	}
	prefix, err := netip.ParsePrefix(nasRoutePrefix())
	if err != nil {
		s.Close()
		return fmt.Errorf("bad JM_NET_NAS_ROUTE %q: %w", nasRoutePrefix(), err)
	}
	lc, err := s.LocalClient()
	if err != nil {
		s.Close()
		return fmt.Errorf("nas node local client: %w", err)
	}
	if _, err := lc.EditPrefs(ctx, &ipn.MaskedPrefs{
		Prefs:              ipn.Prefs{AdvertiseRoutes: []netip.Prefix{prefix}},
		AdvertiseRoutesSet: true,
	}); err != nil {
		s.Close()
		return fmt.Errorf("nas node advertise %s: %w", prefix, err)
	}
	n.srv = s
	go n.approveLoop(prefix.String())
	log.Printf("JuiceMount Link: nas node %q up, advertising %s", s.Hostname, prefix)
	return nil
}

// approveLoop approves the advertised route server-side until it sticks.
// The route only appears in headscale's directory once the node's
// registration fully propagates, hence the retries (~2 min ceiling).
func (n *nasNodeSupervisor) approveLoop(prefix string) {
	cfgPath := hsDataDir + "/config.yaml"
	for tries := 0; tries < 24; tries++ {
		err := manager.ApproveSubnetRoutes(hsBinary, cfgPath, nasNodeHostname(), []string{prefix})
		if err == nil {
			log.Printf("JuiceMount Link: subnet route %s approved for %q", prefix, nasNodeHostname())
			return
		}
		time.Sleep(5 * time.Second)
	}
	log.Printf("JuiceMount Link: WARNING could not approve subnet route %s (linked Macs will not reach LAN services); retry via POST /api/net/routes/approve", prefix)
}

// Stop tears the node down on manager shutdown.
func (n *nasNodeSupervisor) Stop() {
	if n.srv != nil {
		n.srv.Close()
	}
}
