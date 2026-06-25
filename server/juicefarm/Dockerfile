# JuiceMount server-side generation farm (juicefarm).
#
# Runs the PORTABLE derivative + AI generators server-side: ffprobe tech,
# poster/filmstrip/waveform, and whisper.cpp transcripts → written as Tier-A
# blobs into <volume>/.juicemount/derivatives/<inode>/ (shared across every
# client) plus a server-side index. Replaces on-device generation; OpenLoupe
# falls back to local gen only when a derivative is absent.
#
# Built ON the TrueNAS host (native x86_64) — no cross-compile, no registry:
#   docker build -f server/juicefarm/Dockerfile -t juicefarm:local .
# then run additively on the ix-juicemount network (see server/docker-compose.yml).

# ----- Stage 1: jmfarm (Go, CGO for SQLite) -----
FROM golang:1.26-bookworm AS gobuild
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git \
    && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# Static-link (CGO sqlite) so the binary carries no glibc version dependency —
# the juicedata/mount runtime ships an older glibc than the bookworm builder.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux go build -tags sqlite_omit_load_extension \
      -ldflags="-s -w -linkmode external -extldflags '-static'" -o /out/jmfarm ./cmd/jmfarm

# ----- Stage 2: whisper.cpp (whisper-cli) + the ggml model -----
# Bullseye matches the juicedata/mount runtime's glibc 2.31 + libstdc++, so
# whisper-cli links cleanly there (BUILD_SHARED_LIBS=OFF folds in whisper/ggml).
FROM debian:bullseye AS whisper
# GPU-readiness seam: the default CPU build matches the juicedata/mount runtime.
# For a GPU NAS (CUDA/Vulkan APU), build a variant with WHISPER_CMAKE_FLAGS, e.g.
#   --build-arg WHISPER_CMAKE_FLAGS="-DGGML_CUDA=ON"   (needs an nvidia/cuda base
# runtime + the toolkit) or "-DGGML_VULKAN=ON". Encode acceleration is a separate
# axis (ffmpeg hwaccel — see JM_FARM_VCODEC, h264_nvenc/qsv/vaapi). Kept CPU-default
# until the deploy host actually has a GPU; the constants/flags are the only change.
ARG WHISPER_CMAKE_FLAGS=""
# Default model baked into the image (manager-overridable / fetch-on-demand below).
ARG WHISPER_MODEL=medium.en
RUN apt-get update && apt-get install -y --no-install-recommends \
        git cmake build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 https://github.com/ggml-org/whisper.cpp /w \
    && cd /w \
    && cmake -B build -DWHISPER_BUILD_TESTS=OFF -DGGML_NATIVE=OFF -DBUILD_SHARED_LIBS=OFF ${WHISPER_CMAKE_FLAGS} \
    && cmake --build build --config Release -j "$(nproc)" \
    && cp build/bin/whisper-cli /usr/local/bin/whisper-cli
# Bake the default model (medium.en ~1.5GB — good accuracy on the NAS); keep the
# download script so the entrypoint can fetch a different model on demand.
RUN cd /w && bash ./models/download-ggml-model.sh ${WHISPER_MODEL} \
    && cp models/ggml-${WHISPER_MODEL}.bin /ggml-${WHISPER_MODEL}.bin \
    && cp models/download-ggml-model.sh /download-ggml-model.sh

# ----- Stage 3: runtime (juicefs + ffmpeg + whisper.cpp + jmfarm) -----
# juicedata/mount gives /usr/local/bin/juicefs for the in-container FUSE mount.
FROM juicedata/mount:ce-v1.3.1
ARG WHISPER_MODEL=medium.en
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=gobuild /out/jmfarm /usr/local/bin/jmfarm
COPY --from=whisper /usr/local/bin/whisper-cli /usr/local/bin/whisper-cli
COPY --from=whisper /ggml-${WHISPER_MODEL}.bin /models/ggml-${WHISPER_MODEL}.bin
COPY --from=whisper /download-ggml-model.sh /usr/local/bin/download-ggml-model.sh
COPY server/juicefarm/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/download-ggml-model.sh
# Manager-overridable: JM_FARM_MODEL = a baked path (default) OR a bare model name
# (e.g. "large-v3") which the entrypoint fetches into /state/models on first use.
ENV JM_FARM_MODEL=/models/ggml-medium.en.bin
ENTRYPOINT ["/entrypoint.sh"]
