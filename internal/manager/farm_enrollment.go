package manager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/lelanddutcher/juicemount/internal/linkconfig"
	buildversion "github.com/lelanddutcher/juicemount/internal/version"
)

type farmEnrollmentRequest struct {
	Name      string `json:"name"`
	Role      string `json:"role"` // server|render
	StatePath string `json:"state_path"`
	CachePath string `json:"cache_path"`
	Remote    bool   `json:"remote"`
}

type farmEnrollmentPlan struct {
	Name             string   `json:"name"`
	Role             string   `json:"role"`
	Image            string   `json:"image"`
	ExpectedCommit   string   `json:"expected_commit"`
	PrepareCommand   string   `json:"prepare_command"`
	DockerCommand    string   `json:"docker_command"`
	RecoveryUnit     string   `json:"recovery_unit"`
	RecoveryCommand  string   `json:"recovery_command"`
	LinkCommand      string   `json:"link_command,omitempty"`
	PairingExpiresIn string   `json:"pairing_expires_in,omitempty"`
	Checks           []string `json:"checks"`
}

// handleFarmEnrollment creates a role-specific, copyable deployment plan. The
// backend URL and optional one-time Link key are returned only to an
// authenticated administrator and every response is explicitly no-store.
// Nothing is executed on the target host: that final trust boundary stays with
// the operator, while the generated command pins the expected RC image/name,
// persistence, restart policy, FUSE access, and render devices consistently.
func (a *API) handleFarmEnrollment(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store, max-age=0")
	w.Header().Set("Pragma", "no-cache")
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if a.farmQ == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "farm queue not configured"})
		return
	}
	metaURL := strings.TrimSpace(a.farmNodeMetaURL)
	if err := validateFarmNodeMetaURL(metaURL); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"ok":    false,
			"error": "processing-node enrollment is not configured: set JM_FARM_NODE_META_URL to the farm principal's endpoint nodes can reach, without a password (redis://farm@<host>:30179/1): " + err.Error(),
		})
		return
	}
	var req farmEnrollmentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	req.Name = strings.ToLower(strings.TrimSpace(req.Name))
	req.Role = strings.ToLower(strings.TrimSpace(req.Role))
	if !validWorkerName(req.Name) {
		http.Error(w, "invalid stable worker name", http.StatusBadRequest)
		return
	}
	if req.Role != "server" && req.Role != "render" {
		http.Error(w, "role must be server or render", http.StatusBadRequest)
		return
	}
	if req.StatePath == "" {
		req.StatePath = filepath.Join("/var/lib/juicemount", req.Name, "state")
	}
	if req.CachePath == "" {
		req.CachePath = filepath.Join("/var/cache/juicemount", req.Name)
	}
	if !validFarmHostDir(req.StatePath) || !validFarmHostDir(req.CachePath) || req.StatePath == req.CachePath {
		http.Error(w, "state_path and cache_path must be distinct clean absolute host directories", http.StatusBadRequest)
		return
	}

	image := a.farmServerImage
	kinds := "all"
	transcriptDevice := "cpu"
	vcodec := "libx264"
	if req.Role == "render" {
		image = a.farmRenderImage
		kinds = "derivatives,proxy,transcript"
		transcriptDevice = "vulkan"
		vcodec = "hevc_vaapi"
	}
	if image == "" {
		image = defaultFarmEnrollmentImage(req.Role)
	}
	if strings.ContainsFunc(image, unicode.IsControl) {
		http.Error(w, "configured worker image is invalid", http.StatusInternalServerError)
		return
	}

	args := []string{
		"docker", "run", "-d",
		"--name", "juicefarm-" + req.Name,
		"--hostname", req.Name,
		"--restart", "unless-stopped",
		"--network", "host",
		"--cap-add", "SYS_ADMIN",
		"--security-opt", "apparmor=unconfined",
		"--device", "/dev/fuse",
		"--label", "com.juicemount.worker-name=" + req.Name,
		"--label", "com.juicemount.commit=" + buildversion.Commit,
		"-v", req.StatePath + ":/state",
		"-v", req.CachePath + ":/jfs-cache",
		"-e", "JM_META=" + metaURL,
		// H-8B: the farm principal's secret is copied from the operator's
		// shell (`-e NAME` without a value), never typed into this command:
		// it stays out of shell history, the node's argv and this response.
		"-e", "JM_META_PASSWORD",
		// H-8B §2.1: the volume's no-delete object key (jm-client), by
		// name only, like the principal secret.
		"-e", "MINIO_ACCESS_KEY",
		"-e", "MINIO_SECRET_KEY",
		"-e", "JM_FARM_QUEUE=1",
		"-e", "JM_WORKER_NAME=" + req.Name,
		"-e", "JM_WORKER_ROLE=" + req.Role,
		"-e", "JM_FARM_KINDS=" + kinds,
		"-e", "JM_FARM_TRANSCRIPT_DEVICE=" + transcriptDevice,
		"-e", "JM_FARM_VCODEC=" + vcodec,
	}
	if req.Role == "render" {
		args = append(args, "--device", "/dev/dri")
	}
	args = append(args, image)

	recoveryUnit := farmWorkerRecoveryUnit(req.Name, req.Role)
	plan := farmEnrollmentPlan{
		Name:            req.Name,
		Role:            req.Role,
		Image:           image,
		ExpectedCommit:  buildversion.Commit,
		PrepareCommand:  shellJoin([]string{"sudo", "install", "-d", "-m", "0700", req.StatePath, req.CachePath}),
		DockerCommand:   shellJoin(args),
		RecoveryUnit:    recoveryUnit,
		RecoveryCommand: farmWorkerRecoveryInstallCommand(req.Name, recoveryUnit),
		Checks: []string{
			"Before the docker command, put the farm principal's secret (JM_PROXY_FARM_PASSWORD from the NAS server/.env) in the shell without echo: read -rs JM_META_PASSWORD && export JM_META_PASSWORD. Unset it afterwards. Never paste it into the command line.",
			"Also export the no-delete object key from the NAS server/.env the same way: MINIO_ACCESS_KEY=jm-client (JM_OBJ_CLIENT_ACCESS_KEY) and read -rs MINIO_SECRET_KEY (JM_OBJ_CLIENT_SECRET_KEY) && export MINIO_ACCESS_KEY MINIO_SECRET_KEY. Never the jm-gc key: a node must not delete objects.",
			"Load or pull the exact image shown before running the command.",
			"Confirm the node heartbeat reports the expected full commit in Manager.",
			"Render admission is successful only when live encode, decode, mount-access, and AI probes pass.",
			"Install the recovery unit as /etc/systemd/system/juicefarm-" + req.Name + ".service, then daemon-reload and enable it with --now; leave it disabled to keep the node stopped.",
		},
	}
	if req.Remote {
		serverURL := strings.TrimRight(envString("JM_NET_SERVER_URL", ""), "/")
		if !linkConfigured() || strings.TrimSpace(serverURL) == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "JuiceMount Link is not configured for remote node enrollment"})
			return
		}
		if err := linkconfig.ValidateServerURL(
			serverURL,
			os.Getenv("JM_NET_ALLOW_INSECURE") == "1",
			os.Getenv("JM_NET_ALLOW_LAN_ONLY") == "1",
		); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok":    false,
				"error": "JuiceMount Link is not remote-ready for node enrollment: " + err.Error(),
			})
			return
		}
		if !linkconfig.RemoteReady(serverURL) {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{
				"ok":    false,
				"error": "JuiceMount Link is LAN-only; remote node enrollment requires a public HTTPS coordination URL",
			})
			return
		}
		pairingCode, err := mintLinkPreauthKey(false)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": "could not mint one-time Link authorization: " + err.Error()})
			return
		}
		plan.LinkCommand = shellJoin([]string{
			"sudo", "tailscale", "up", "--login-server", serverURL,
			"--auth-key", pairingCode, "--hostname", req.Name, "--accept-routes",
		})
		plan.PairingExpiresIn = "1h"
		plan.Checks = append([]string{"Run the one-time Link command first and verify the NAS route is reachable."}, plan.Checks...)
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "plan": plan})
}

// farmWorkerRecoveryUnit makes reboot recovery part of every generated worker
// enrollment plan. Docker's unless-stopped policy alone can leave a standalone
// worker stopped after a clean daemon shutdown; the supervisor unit keeps the
// worker process under an explicit supervisor while preserving an intentional
// operator stop. The running-container branch makes installation idempotent
// when the docker run command has already started the worker.
func farmWorkerRecoveryUnit(name, role string) string {
	container := "juicefarm-" + name
	return fmt.Sprintf(`[Unit]
Description=JuiceFarm RC worker (%s)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=:/bin/sh -c 'running=$(/usr/bin/docker inspect -f "{{.State.Running}}" %s) || exit 1; if [ "$running" != true ]; then /usr/bin/docker start %s || exit 1; fi; status=$(/usr/bin/docker wait %s) || exit 1; exit "$status"'
ExecStop=/usr/bin/docker stop --time=30 %s
Restart=on-failure
RestartSec=15
TimeoutStartSec=0
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
`, role, container, container, container, container)
}

func farmWorkerRecoveryInstallCommand(name, unit string) string {
	service := "juicefarm-" + name + ".service"
	return fmt.Sprintf("cat <<'UNIT' | sudo tee /etc/systemd/system/%s >/dev/null\n%sUNIT\nsudo systemctl daemon-reload\nsudo systemctl enable --now %s", service, unit, service)
}

// validateFarmNodeMetaURL accepts only user@host: the URL is printed into
// a docker command, so it must carry no secret (H-8B). Nodes receive the
// farm principal's password as JM_META_PASSWORD from the operator's shell.
func validateFarmNodeMetaURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Hostname() == "" {
		return fmt.Errorf("invalid worker Redis endpoint")
	}
	if strings.ContainsFunc(raw, unicode.IsControl) {
		return fmt.Errorf("invalid worker Redis endpoint")
	}
	if u.User == nil || u.User.Username() == "" {
		return fmt.Errorf("the endpoint must name the farm principal (redis://farm@<host>:30179/1)")
	}
	if _, has := u.User.Password(); has {
		return fmt.Errorf("the endpoint must not carry a password")
	}
	return nil
}

func validFarmHostDir(path string) bool {
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path || path == "/" {
		return false
	}
	return !strings.ContainsFunc(path, unicode.IsControl)
}

func defaultFarmEnrollmentImage(role string) string {
	base := "juicefarm"
	if role == "render" {
		base = "juicefarm-gpu"
	}
	commit := strings.TrimSpace(buildversion.Commit)
	if len(commit) >= 7 && commit != "dev" {
		return base + ":rc-0.5-" + commit[:7]
	}
	return base + ":local"
}

func shellJoin(args []string) string {
	quoted := make([]string, 0, len(args))
	for _, arg := range args {
		quoted = append(quoted, shellQuote(arg))
	}
	return strings.Join(quoted, " ")
}

func shellQuote(s string) string {
	if s != "" && !strings.ContainsAny(s, " \t\r\n'\"\\$`;&|<>()[]{}*?!") {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}
