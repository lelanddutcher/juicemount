package juicefarm

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

var pinnedSHAArg = regexp.MustCompile(`(?m)^ARG ([A-Z_]+_COMMIT)=([0-9a-f]{40})$`)

func dockerfile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func commitArg(t *testing.T, src, name string) string {
	t.Helper()
	for _, match := range pinnedSHAArg.FindAllStringSubmatch(src, -1) {
		if match[1] == name {
			return match[2]
		}
	}
	t.Fatalf("%s is not pinned to a full commit SHA", name)
	return ""
}

func TestWorkerDockerfilesPinSourceAndBakedModel(t *testing.T) {
	cpu := dockerfile(t, "Dockerfile")
	gpu := dockerfile(t, "../juicefarm-gpu/Dockerfile")

	cpuWhisper := commitArg(t, cpu, "WHISPER_CPP_COMMIT")
	gpuWhisper := commitArg(t, gpu, "WHISPER_CPP_COMMIT")
	if cpuWhisper != gpuWhisper {
		t.Fatalf("CPU/GPU whisper revisions differ: %s != %s", cpuWhisper, gpuWhisper)
	}
	for name, src := range map[string]string{"CPU": cpu, "GPU": gpu} {
		if strings.Contains(src, "git clone --depth 1 https://github.com/ggml-org/whisper.cpp") {
			t.Fatalf("%s worker regressed to an unpinned whisper.cpp clone", name)
		}
		if !strings.Contains(src, "fetch --depth 1 origin ${WHISPER_CPP_COMMIT}") ||
			!strings.Contains(src, `test "$(git -C /w rev-parse HEAD)" = "${WHISPER_CPP_COMMIT}"`) {
			t.Fatalf("%s worker does not fetch and verify its pinned whisper.cpp commit", name)
		}
		if !strings.Contains(src, "ARG WHISPER_MODEL_SHA256=") ||
			!strings.Contains(src, "${WHISPER_MODEL_SHA256}  models/ggml-${WHISPER_MODEL}.bin") {
			t.Fatalf("%s worker does not checksum its baked speech model", name)
		}
	}

	commitArg(t, gpu, "SPIRV_HEADERS_COMMIT")
	commitArg(t, gpu, "VULKAN_HEADERS_COMMIT")
}

func TestCPUWorkerUsesModernFFmpegRuntime(t *testing.T) {
	cpu := dockerfile(t, "Dockerfile")

	for _, required := range []string{
		"FROM juicedata/mount:ce-v1.3.1 AS juicefs",
		"FROM debian:trixie-slim",
		"COPY --from=juicefs /usr/local/bin/juicefs /usr/local/bin/juicefs",
		"COPY --from=juicefs /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so",
	} {
		if !strings.Contains(cpu, required) {
			t.Fatalf("CPU worker runtime no longer contains %q", required)
		}
	}
	if strings.Contains(cpu, "FROM juicedata/mount:ce-v1.3.1\n") {
		t.Fatal("CPU worker regressed to the Debian 11 JuiceFS runtime with ffmpeg 4.3")
	}
}

func TestWorkerImagesRequireAndVerifyExactReleaseIdentity(t *testing.T) {
	for name, src := range map[string]string{
		"CPU": dockerfile(t, "Dockerfile"),
		"GPU": dockerfile(t, "../juicefarm-gpu/Dockerfile"),
	} {
		for _, forbidden := range []string{"ARG JM_COMMIT=unknown", "ARG JM_VERSION=0.5.0"} {
			if strings.Contains(src, forbidden) {
				t.Fatalf("%s worker still permits default release identity %q", name, forbidden)
			}
		}
		if !strings.Contains(src, `grep -Eq '^[0-9a-f]{40}$'`) {
			t.Fatalf("%s worker does not reject a missing or abbreviated commit", name)
		}
		if !strings.Contains(src, `jmfarm --build-info`) {
			t.Fatalf("%s worker does not execute its final binary to verify embedded identity", name)
		}
	}
}
