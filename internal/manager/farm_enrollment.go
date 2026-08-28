package manager

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"unicode"

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
			"error": "processing-node enrollment is not configured: set JM_FARM_NODE_META_URL to the Redis endpoint nodes can reach",
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

	plan := farmEnrollmentPlan{
		Name:           req.Name,
		Role:           req.Role,
		Image:          image,
		ExpectedCommit: buildversion.Commit,
		PrepareCommand: shellJoin([]string{"sudo", "install", "-d", "-m", "0700", req.StatePath, req.CachePath}),
		DockerCommand:  shellJoin(args),
		Checks: []string{
			"Load or pull the exact image shown before running the command.",
			"Confirm the node heartbeat reports the expected full commit in Manager.",
			"Render admission is successful only when live encode, decode, mount-access, and AI probes pass.",
		},
	}
	if req.Remote {
		if !linkConfigured() || strings.TrimSpace(strings.TrimRight(envString("JM_NET_SERVER_URL", ""), "/")) == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": "JuiceMount Link is not configured for remote node enrollment"})
			return
		}
		pairingCode, err := mintLinkPreauthKey(false)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": "could not mint one-time Link authorization: " + err.Error()})
			return
		}
		serverURL := strings.TrimRight(envString("JM_NET_SERVER_URL", ""), "/")
		plan.LinkCommand = shellJoin([]string{
			"sudo", "tailscale", "up", "--login-server", serverURL,
			"--auth-key", pairingCode, "--hostname", req.Name, "--accept-routes",
		})
		plan.PairingExpiresIn = "1h"
		plan.Checks = append([]string{"Run the one-time Link command first and verify the NAS route is reachable."}, plan.Checks...)
	}

	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "plan": plan})
}

func validateFarmNodeMetaURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "redis" && u.Scheme != "rediss") || u.Hostname() == "" {
		return fmt.Errorf("invalid worker Redis endpoint")
	}
	if strings.ContainsFunc(raw, unicode.IsControl) {
		return fmt.Errorf("invalid worker Redis endpoint")
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
