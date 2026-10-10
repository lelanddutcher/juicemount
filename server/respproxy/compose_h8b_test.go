package respproxydeploy

// Contract tests for the H-8 option B server deployment (plan §1.6, §3).
// They read the canonical compose and Redis files as text: the NAS deploys
// exactly these files, so their content is the contract.

import (
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// serviceBlock returns the lines of one top-level compose service.
func serviceBlock(t *testing.T, compose, name string) string {
	t.Helper()
	lines := strings.Split(compose, "\n")
	start := -1
	for i, l := range lines {
		if l == "  "+name+":" {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("compose has no service %q", name)
	}
	var out []string
	for _, l := range lines[start+1:] {
		if strings.HasPrefix(l, "  ") && !strings.HasPrefix(l, "   ") && strings.TrimSpace(l) != "" && !strings.HasPrefix(strings.TrimSpace(l), "#") {
			break // next service
		}
		if l != "" && !strings.HasPrefix(l, " ") {
			break // next top-level key
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

// keyBlock returns the indented lines under "    <key>:" in a service block.
func keyBlock(block, key string) (string, bool) {
	lines := strings.Split(block, "\n")
	for i, l := range lines {
		if strings.TrimSpace(l) == key+":" && strings.HasPrefix(l, "    ") && !strings.HasPrefix(l, "     ") {
			var out []string
			for _, m := range lines[i+1:] {
				if strings.TrimSpace(m) == "" || strings.HasPrefix(strings.TrimSpace(m), "#") {
					continue
				}
				if !strings.HasPrefix(m, "      ") {
					break
				}
				out = append(out, m)
			}
			return strings.Join(out, "\n"), true
		}
	}
	return "", false
}

func uncomment(s string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(l), "#") {
			continue
		}
		if i := strings.Index(l, " #"); i >= 0 {
			l = l[:i]
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

// TestH8BComposeRedisUnpublishedDefaultOff: after the cutover the metadata
// Redis is reachable only through the proxy. Redis publishes no host port,
// refuses unauthenticated clients (default off), and has exactly two
// users: jm-proxy (all commands, keys and channels: Redis 7 defaults to
// resetchannels, and keyspace push needs PSUBSCRIBE) and a jm-admin whose
// secret never leaves the Redis container. The proxy owns host port 30179
// and every in-stack client reaches the volume through it, each under its
// own principal, with every secret coming from server/.env.
func TestH8BComposeRedisUnpublishedDefaultOff(t *testing.T) {
	compose := readFile(t, "../docker-compose.yml")
	code := uncomment(compose)

	redis := uncomment(serviceBlock(t, compose, "redis"))
	if _, published := keyBlock(redis, "ports"); published {
		t.Fatalf("redis still publishes a host port:\n%s", redis)
	}
	if strings.Contains(redis, "30179") {
		t.Fatal("redis still references the client port 30179")
	}
	if strings.Contains(redis, "--requirepass") || strings.Contains(redis, "REDIS_PASSWORD:-") {
		t.Fatal("redis still uses the optional shared requirepass (empty = no auth)")
	}
	if !strings.Contains(redis, "--aclfile") {
		t.Fatal("redis does not load a rendered ACL file")
	}
	if !strings.Contains(redis, "${JM_REDIS_PROXY_PASSWORD:?") {
		t.Fatal("redis must require the jm-proxy upstream secret from server/.env")
	}

	// ACL lines use " #<sha256>" for passwords: strip whole comment lines only.
	var aclLines []string
	for _, l := range strings.Split(readFile(t, "../redis.acl"), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(l), "#") {
			aclLines = append(aclLines, l)
		}
	}
	acl := strings.Join(aclLines, "\n")
	userLine := func(name string) string {
		for _, l := range strings.Split(acl, "\n") {
			f := strings.Fields(l)
			if len(f) >= 2 && f[0] == "user" && f[1] == name {
				return l
			}
		}
		return ""
	}
	def := userLine("default")
	if def == "" || !strings.Contains(" "+def+" ", " off ") || strings.Contains(def, " on ") || strings.Contains(def, "nopass") {
		t.Fatalf("redis.acl default user must be off: %q", def)
	}
	proxy := userLine("jm-proxy")
	for _, want := range []string{" on ", "+@all", "~*", "&*"} {
		if !strings.Contains(proxy+" ", want) {
			t.Fatalf("jm-proxy ACL %q lacks %q (Redis 7 defaults to resetchannels: keyspace push needs &*)", proxy, want)
		}
	}
	admin := userLine("jm-admin")
	if admin == "" || !strings.Contains(admin, " on ") {
		t.Fatalf("redis.acl lacks the loopback admin user: %q", admin)
	}
	for _, l := range []string{proxy, admin} {
		if strings.Contains(l, "nopass") || regexp.MustCompile(` >[^ ]`).MatchString(l) {
			t.Fatalf("ACL line carries a plaintext or empty password: %q", l)
		}
		if !strings.Contains(l, "#@") {
			t.Fatalf("ACL line must use a sha256 placeholder rendered at start: %q", l)
		}
	}
	var users []string
	for _, l := range strings.Split(acl, "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == "user" {
			users = append(users, f[1])
		}
	}
	if strings.Join(users, ",") != "default,jm-proxy,jm-admin" {
		t.Fatalf("redis.acl users = %v, want exactly default, jm-proxy, jm-admin", users)
	}
	// jm-admin is loopback only: its secret is minted inside the Redis
	// container and must not be supplied by, or exported to, any service.
	if strings.Contains(code, "JM_REDIS_ADMIN_PASSWORD") {
		t.Fatal("jm-admin's secret is configured from outside the Redis container")
	}

	rp := uncomment(serviceBlock(t, compose, "respproxy"))
	ports, ok := keyBlock(rp, "ports")
	if !ok || !strings.Contains(ports, `"30179:6379"`) {
		t.Fatalf("respproxy must take host port 30179:\n%s", rp)
	}
	if !strings.Contains(rp, "jm-respproxy@${JM_RESPPROXY_DIGEST:?") {
		t.Fatal("respproxy image must be pinned by release digest")
	}
	if !strings.Contains(rp, "JM_RESPPROXY_UPSTREAM_PASSWORD: \"${JM_REDIS_PROXY_PASSWORD:?") {
		t.Fatal("respproxy must receive the jm-proxy upstream secret through its environment")
	}
	if !strings.Contains(rp, "JM_RESPPROXY_MODE: \"${JM_RESPPROXY_MODE:-passthrough}\"") ||
		!strings.Contains(rp, "JM_RESPPROXY_STAGE_BIRTH: \"${JM_RESPPROXY_STAGE_BIRTH:-false}\"") {
		t.Fatal("respproxy must default to passthrough with stage births off")
	}
	example := readFile(t, "../release.env.example")
	if !strings.Contains(example, "JM_RESPPROXY_MODE=passthrough") ||
		!strings.Contains(example, "JM_RESPPROXY_STAGE_BIRTH=false") ||
		!strings.Contains(example, "JM_RESPPROXY_MODE=enforce and JM_RESPPROXY_STAGE_BIRTH=true") ||
		!strings.Contains(example, "effective mode override") ||
		!strings.Contains(example, "stage operations HOLD") {
		t.Fatal("release example must describe default-off and both enforce prerequisites")
	}

	// Only the proxy publishes 30179.
	if n := strings.Count(code, "30179:"); n != 1 {
		t.Fatalf("host port 30179 published %d times, want once (the proxy)", n)
	}

	// Every volume client in the stack goes through the proxy with its own
	// principal; the password comes from .env (inline for go-redis users,
	// META_PASSWORD for juicefs processes so it stays out of argv).
	metaRe := regexp.MustCompile(`redis://([^"'\s]*)`)
	principals := map[string]bool{}
	for _, m := range metaRe.FindAllStringSubmatch(code, -1) {
		url := m[1]
		if !strings.Contains(url, "@respproxy:6379/1") {
			t.Fatalf("volume URL %q bypasses the proxy", m[0])
		}
		userinfo := strings.SplitN(url, "@", 2)[0]
		user := strings.SplitN(userinfo, ":", 2)[0]
		principals[user] = true
		// No URL carries a password at all: URLs reach argv (--meta,
		// juicefs, jmfarm -meta); secrets travel in the environment.
		if strings.Contains(userinfo, ":") {
			t.Fatalf("volume URL %q carries a password; use the *_PASSWORD environment instead", m[0])
		}
	}
	for _, want := range []string{"farm", "manager", "bg", "webdav", "admin"} {
		if !principals[want] {
			t.Fatalf("no in-stack client uses principal %q (got %v)", want, principals)
		}
	}
	for _, svc := range []string{"juicefarm", "juicemount-manager"} {
		b := uncomment(serviceBlock(t, compose, svc))
		if !strings.Contains(b, `JM_META: "redis://`) || !strings.Contains(b, "@respproxy:6379/1") {
			t.Fatalf("%s JM_META does not point at the proxy:\n%s", svc, b)
		}
	}
	for _, v := range []string{"JM_PROXY_FARM_PASSWORD", "JM_PROXY_MANAGER_PASSWORD", "JM_PROXY_BG_PASSWORD",
		"JM_PROXY_WEBDAV_PASSWORD", "JM_PROXY_ADMIN_PASSWORD", "JM_REDIS_PROXY_PASSWORD", "JM_RESPPROXY_DIGEST"} {
		if !strings.Contains(code, "${"+v+":?") {
			t.Fatalf("compose must require %s from server/.env", v)
		}
		ex := readFile(t, "../release.env.example")
		if !regexp.MustCompile(`(?m)^` + v + `=(sha256:)?REPLACE_`).MatchString(ex) {
			t.Fatalf("release.env.example must carry a placeholder for %s", v)
		}
	}
	// Server mounts that are not the background-job owner must not run
	// background jobs (the proxy refuses their writes as BG_ONLY).
	if !regexp.MustCompile(`juicefs webdav[^&]*--no-bgjob`).MatchString(code) {
		t.Fatal("the WebDAV gateway must run with --no-bgjob")
	}
	for _, m := range metaRe.FindAllStringSubmatch(readFile(t, "../release.env.example"), -1) {
		if strings.Contains(strings.SplitN(m[1], "@", 2)[0], ":") {
			t.Fatalf("release.env.example tells the operator to put a password in %q", m[0])
		}
	}
	if !strings.Contains(redis, "stop_grace_period: 120s") {
		t.Fatal("redis needs a 120s stop grace period so the RDB save on SIGTERM is not SIGKILLed")
	}
	for _, df := range []string{"../juicemount-manager/Dockerfile", "../juicemount-manager/Dockerfile.binary-only"} {
		if !strings.Contains(readFile(t, df), "must not carry a password") {
			t.Fatalf("%s must refuse a password inline in JM_META (it reaches --meta argv)", df)
		}
	}
	mgr := uncomment(serviceBlock(t, compose, "juicemount-manager"))
	for _, v := range []string{"JM_META_PASSWORD", "JM_META_BG_PASSWORD", "JM_META_ADMIN_PASSWORD"} {
		if !strings.Contains(mgr, v+":") {
			t.Fatalf("Manager lacks %s", v)
		}
	}
	farmEntry := readFile(t, "../juicefarm/entrypoint.sh")
	if strings.Contains(farmEntry, `-meta "$JM_META"`) {
		t.Fatal("jmfarm still gets the raw JM_META (with any password) in argv")
	}
	if !strings.Contains(farmEntry, "--no-bgjob") {
		t.Fatal("the farm mount must run with --no-bgjob")
	}
}

// TestH8BManagerImageRefusesInlinePassword runs the Manager image's
// entrypoint guard itself (not just its message) against URLs: user:pw@
// and :pw@ refuse, user@ and no userinfo pass.
func TestH8BManagerImageRefusesInlinePassword(t *testing.T) {
	re := regexp.MustCompile(`case \\"\$u\\" in ([^)]*)\)`)
	for _, df := range []string{"../juicemount-manager/Dockerfile", "../juicemount-manager/Dockerfile.binary-only"} {
		m := re.FindStringSubmatch(readFile(t, df))
		if m == nil {
			t.Fatalf("%s: no JM_META password guard found", df)
		}
		pattern := m[1]
		for url, refuse := range map[string]bool{
			"redis://manager:pw@respproxy:6379/1": true,
			"redis://:pw@respproxy:6379/1":        true,
			"rediss://manager:pw@h:30179/1":       true,
			"redis://manager@respproxy:6379/1":    false,
			"redis://respproxy:6379/1":            false,
			"":                                    false,
		} {
			script := `case "$1" in ` + pattern + `) exit 3 ;; esac; exit 0`
			err := exec.Command("/bin/sh", "-c", script, "guard", url).Run()
			refused := err != nil
			if refused != refuse {
				t.Errorf("%s pattern %q on %q: refused=%v, want %v", df, pattern, url, refused, refuse)
			}
		}
	}
}
