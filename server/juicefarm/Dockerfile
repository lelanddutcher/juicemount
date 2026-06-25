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
RUN apt-get update && apt-get install -y --no-install-recommends \
        git cmake build-essential ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
RUN git clone --depth 1 https://github.com/ggml-org/whisper.cpp /w \
    && cd /w \
    && cmake -B build -DWHISPER_BUILD_TESTS=OFF -DGGML_NATIVE=OFF -DBUILD_SHARED_LIBS=OFF \
    && cmake --build build --config Release -j "$(nproc)" \
    && cp build/bin/whisper-cli /usr/local/bin/whisper-cli
# Bake the base.en model (~142MB) so the image is self-contained.
RUN cd /w && bash ./models/download-ggml-model.sh base.en && cp models/ggml-base.en.bin /ggml-base.en.bin

# ----- Stage 3: runtime (juicefs + ffmpeg + whisper.cpp + jmfarm) -----
# juicedata/mount gives /usr/local/bin/juicefs for the in-container FUSE mount.
FROM juicedata/mount:ce-v1.3.1
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=gobuild /out/jmfarm /usr/local/bin/jmfarm
COPY --from=whisper /usr/local/bin/whisper-cli /usr/local/bin/whisper-cli
COPY --from=whisper /ggml-base.en.bin /models/ggml-base.en.bin
COPY server/juicefarm/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENV JM_FARM_MODEL=/models/ggml-base.en.bin
ENTRYPOINT ["/entrypoint.sh"]
