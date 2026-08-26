# JuiceMount GPU farm worker (juicefarm-gpu) — Intel Arc / Vulkan edition.
#
# Same shape as server/juicefarm/Dockerfile (jmfarm + whisper.cpp + ffmpeg on
# juicedata/mount), with two deltas:
#   1. whisper.cpp built with -DGGML_VULKAN=ON — runs transcription on any
#      Vulkan GPU (Intel Arc Battlemage included; no CUDA needed).
#   2. Runtime image adds Mesa VAAPI drivers (intel-media-va-driver-non-free,
#      libva) so ffmpeg can use h264_vaapi/hevc_vaapi for proxy encodes, plus vulkan
#      loader/libs for whisper.
#
# Run flags the host MUST provide:
#   --device /dev/dri                      (render node for VAAPI + Vulkan)
#   -v /usr/lib/x86_64-linux-gnu/dri/iHD_drv_video.so:/usr/lib/x86_64-linux-gnu/dri/iHD_drv_video.so:ro
#                                           (use the host iHD driver for newer Intel GPUs)
#   -e JM_FARM_TRANSCRIPT_DEVICE=vulkan    (whisper → GPU)
#   -e JM_FARM_VCODEC=hevc_vaapi           (HEVC proxy encode → GPU; the live
#                                          scheduler selects this only after a
#                                          real encode + decode probe succeeds)
#   -e JM_WORKER_NAME=<unique>             (per-node config identity)
#   -e JM_FARM_KINDS=transcript            (which dedicated queues to drain)
#
# Built ON the target host (native x86_64), same convention as juicefarm:
#   docker build -f server/juicefarm-gpu/Dockerfile -t juicefarm-gpu:local .

# ----- Stage 1: jmfarm (Go, CGO sqlite, static) -----
FROM golang:1.26.6-bookworm AS gobuild
WORKDIR /src
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git \
    && rm -rf /var/lib/apt/lists/*
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 GOOS=linux go build -tags sqlite_omit_load_extension \
      -ldflags="-s -w -linkmode external -extldflags '-static'" -o /out/jmfarm ./cmd/jmfarm

# ----- Stage 2: whisper.cpp with the Vulkan backend -----
FROM debian:bookworm AS whisper
ARG WHISPER_CMAKE_FLAGS="-DGGML_VULKAN=ON"
ARG WHISPER_MODEL=medium.en
RUN apt-get update && apt-get install -y --no-install-recommends \
        git cmake build-essential ca-certificates curl \
        libvulkan-dev vulkan-tools glslc glslang-tools spirv-headers \
        vulkan-validationlayers spirv-tools \
    && rm -rf /var/lib/apt/lists/* \
    && git clone --depth 1 https://github.com/KhronosGroup/SPIRV-Headers /usr/local/share/spirv-headers \
    && git clone --depth 1 https://github.com/KhronosGroup/Vulkan-Headers /usr/local/share/vulkan-headers \
    && cd /usr/local/share/vulkan-headers && cmake -B build -DCMAKE_INSTALL_PREFIX=/usr/local . && cmake --install build
# GGML_VULKAN needs recent Vulkan headers (vk::LayerSettingEXT requires >=1.3.240;
# bookworm's 1.3.239 is too old) + SPIRV-Headers + glslangValidator.
ENV CMAKE_PREFIX_PATH=/usr/local:/usr/local/share/spirv-headers
RUN git clone --depth 1 https://github.com/ggml-org/whisper.cpp /w \
    && cd /w \
    && cmake -B build -DWHISPER_BUILD_TESTS=OFF -DGGML_NATIVE=OFF -DBUILD_SHARED_LIBS=OFF ${WHISPER_CMAKE_FLAGS} \
    && cmake --build build --config Release -j "$(nproc)" \
    && cp build/bin/whisper-cli /usr/local/bin/whisper-cli
RUN cd /w && bash ./models/download-ggml-model.sh ${WHISPER_MODEL} \
    && cp models/ggml-${WHISPER_MODEL}.bin /ggml-${WHISPER_MODEL}.bin \
    && cp models/download-ggml-model.sh /download-ggml-model.sh

# The JuiceFS CLI is static, so copy it into the modern runtime below instead
# of inheriting the upstream image's Debian 11 userland. Intel Battlemage needs
# the current libva/iHD stack; mixing Debian 13 driver packages into that older
# base leaves the driver absent or ABI-incompatible.
FROM juicedata/mount:ce-v1.3.1 AS juicefs

# ----- Stage 3: runtime (JuiceFS + VAAPI-capable ffmpeg + whisper-vulkan + jmfarm) -----
FROM debian:trixie-slim
ARG WHISPER_MODEL=medium.en
RUN apt-get update && apt-get install -y --no-install-recommends \
        ffmpeg ca-certificates fuse3 libgomp1 util-linux \
        libvulkan1 vulkan-tools mesa-vulkan-drivers \
        libva2 libva-drm2 vainfo intel-media-va-driver librados2 libgfapi0 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=juicefs /usr/local/bin/juicefs /usr/local/bin/juicefs
# The otherwise-static JuiceFS binary dlopens FoundationDB's client library at
# startup even for Redis metadata. Carry the compatible library from the same
# upstream image into the modern VAAPI runtime and register it with ldconfig.
COPY --from=juicefs /usr/lib/libfdb_c.so /usr/lib/libfdb_c.so
RUN ldconfig
COPY --from=gobuild /out/jmfarm /usr/local/bin/jmfarm
COPY --from=whisper /usr/local/bin/whisper-cli /usr/local/bin/whisper-cli
COPY --from=whisper /ggml-${WHISPER_MODEL}.bin /models/ggml-${WHISPER_MODEL}.bin
COPY --from=whisper /download-ggml-model.sh /usr/local/bin/download-ggml-model.sh
COPY server/juicefarm/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /usr/local/bin/download-ggml-model.sh
ENV JM_FARM_MODEL=/models/ggml-medium.en.bin
# GPU defaults — overridable. The manager's farm config can flip these live
# per-node anyway (hot knobs); these ENVs are just sane cold-start values.
ENV JM_FARM_TRANSCRIPT_DEVICE=vulkan
ENV JM_FARM_VCODEC=hevc_vaapi
# The iHD VAAPI driver isn't auto-probed on the drm display path inside a
# container; pin it (harmless on non-Intel hosts — vainfo just fails).
ENV LIBVA_DRIVER_NAME=iHD
ENTRYPOINT ["/entrypoint.sh"]
