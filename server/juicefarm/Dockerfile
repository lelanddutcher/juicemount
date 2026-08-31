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
ARG JM_VERSION
ARG JM_COMMIT
FROM golang:1.26.6-bookworm AS gobuild
ARG JM_VERSION
ARG JM_COMMIT
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git \
    && rm -rf /var/lib/apt/lists/*
RUN test -n "$JM_VERSION" \
    && echo "$JM_COMMIT" | grep -Eq '^[0-9a-f]{40}$'
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
# Static-link (CGO sqlite) so the binary carries no glibc version dependency —
# the juicedata/mount runtime ships an older glibc than the bookworm builder.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux go build -tags sqlite_omit_load_extension \
      -ldflags="-s -w -linkmode external -extldflags '-static' -X github.com/lelanddutcher/juicemount/internal/version.Version=${JM_VERSION} -X github.com/lelanddutcher/juicemount/internal/version.Commit=${JM_COMMIT}" -o /out/jmfarm ./cmd/jmfarm

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
ARG WHISPER_CPP_COMMIT=978113305b2ead22249b881deafa131dc8884911
ARG WHISPER_MODEL_SHA256=cc37e93478338ec7700281a7ac30a10128929eb8f427dda2e865faa8f6da4356
RUN apt-get update && apt-get install -y --no-install-recommends \
        git cmake build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN git init /w \
    && git -C /w remote add origin https://github.com/ggml-org/whisper.cpp \
    && git -C /w fetch --depth 1 origin ${WHISPER_CPP_COMMIT} \
    && git -C /w checkout --detach FETCH_HEAD \
    && test "$(git -C /w rev-parse HEAD)" = "${WHISPER_CPP_COMMIT}" \
    && cd /w \
    && cmake -B build -DWHISPER_BUILD_TESTS=OFF -DGGML_NATIVE=OFF -DBUILD_SHARED_LIBS=OFF ${WHISPER_CMAKE_FLAGS} \
    && cmake --build build --config Release -j "$(nproc)" \
    && cp build/bin/whisper-cli /usr/local/bin/whisper-cli
# Bake the default model (medium.en ~1.5GB — good accuracy on the NAS); keep the
# download script so the entrypoint can fetch a different model on demand.
RUN cd /w && bash ./models/download-ggml-model.sh ${WHISPER_MODEL} \
    && echo "${WHISPER_MODEL_SHA256}  models/ggml-${WHISPER_MODEL}.bin" | sha256sum -c - \
    && cp models/ggml-${WHISPER_MODEL}.bin /ggml-${WHISPER_MODEL}.bin \
    && cp models/download-ggml-model.sh /download-ggml-model.sh

# Keep JuiceFS sourced from the pinned upstream image, but do not inherit its
# Debian 11 userland. That image's ffmpeg 4.3 reports Sony `ipcm` tracks as
# unknown, so otherwise-valid camera audio is impossible to waveform and every
# affected derivative shard is marked failed. The GPU worker already uses this
# copy-forward pattern successfully.
FROM juicedata/mount:ce-v1.3.1 AS juicefs
RUN mkdir -p /juicemount-runtime/usr/local/bin /juicemount-runtime/usr/lib \
    && cp /usr/local/bin/juicefs /juicemount-runtime/usr/local/bin/juicefs \
    && if [ -f /usr/lib/libfdb_c.so ]; then \
         cp /usr/lib/libfdb_c.so /juicemount-runtime/usr/lib/libfdb_c.so; \
       fi

# ----- Stage 3: runtime (juicefs + ffmpeg + whisper.cpp + jmfarm) -----
# Debian 13 carries ffmpeg 7.1+, which maps MOV/MP4 `ipcm` to pcm_s24be and can
# decode the real multi-track Sony footage used by the live RC acceptance run.
FROM debian:trixie-slim
ARG WHISPER_MODEL=medium.en
ARG JM_VERSION
ARG JM_COMMIT
LABEL org.opencontainers.image.version="${JM_VERSION}" \
      org.opencontainers.image.revision="${JM_COMMIT}"
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg ca-certificates fuse3 libgomp1 util-linux librados2 libgfapi0 \
    && rm -rf /var/lib/apt/lists/*
# The pinned amd64 JuiceFS image carries FoundationDB's client library because
# that binary can dlopen it at startup. The arm64 image intentionally does not
# carry the library. The normalized stage above copies the library only where
# upstream supplies it, while keeping the JuiceFS binary path identical on both
# architectures.
COPY --from=juicefs /juicemount-runtime/ /
RUN ldconfig
COPY --from=gobuild /out/jmfarm /usr/local/bin/jmfarm
RUN test "$(/usr/local/bin/jmfarm --build-info)" = "jmfarm ${JM_VERSION} (${JM_COMMIT})"
COPY --from=whisper /usr/local/bin/whisper-cli /usr/local/bin/whisper-cli
COPY --from=whisper /ggml-${WHISPER_MODEL}.bin /models/ggml-${WHISPER_MODEL}.bin
COPY --from=whisper /download-ggml-model.sh /usr/local/bin/download-ggml-model.sh
COPY server/juicefarm/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/download-ggml-model.sh
# Manager-overridable: JM_FARM_MODEL = a baked path (default) OR a bare model name
# (e.g. "large-v3") which the entrypoint fetches into /state/models on first use.
ENV JM_FARM_MODEL=/models/ggml-medium.en.bin
ENV JM_FARM_ENCODE_SCRATCH=/tmp
ENTRYPOINT ["/entrypoint.sh"]
