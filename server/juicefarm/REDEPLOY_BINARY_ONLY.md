# Redeploying the farm when only the Go binary changed

The full `server/juicefarm/Dockerfile` compiles whisper.cpp from source. For a
change that touches only Go code, rebuilding all of that is a long build with a
large blast radius, and the farm has previously been down 37 hours after a
redeploy went wrong. This is the light path: rebuild **only** the Go stage and
layer the new `jmfarm` over the proven runtime image.

Used successfully on 2026-08-12 to ship the derivative-directory ownership fix.

---

## The trap that will bite you

**The worker runs with `--security-opt apparmor=unconfined`, and without it
`fusermount` fails with `Permission denied` and the volume never mounts.**

`docker inspect` with a hand-picked `--format` template will not show you this
unless you ask for `.HostConfig.SecurityOpt` by name. Omitting it is exactly how
the first attempt at this deploy failed: the container started, looked healthy
in `docker ps`, and had no `/jfs` at all. Capture the WHOLE `HostConfig`, or at
minimum every field in the run command below.

The full set the worker needs:

| flag | why |
|---|---|
| `--cap-add CAP_SYS_ADMIN` | mount(2) |
| `--security-opt apparmor=unconfined` | **fusermount; the one that is easy to miss** |
| `--device /dev/fuse` | FUSE |
| `--network ix-juicemount_default` | reaches `redis:6379` and `minio:9000` by name |
| `--restart unless-stopped` | matches how it was deployed |

It is a **standalone container**, not part of the compose app. Never re-apply
the ix-juicemount app YAML to change it — that restarts redis and minio, which
the Mac client depends on.

---

## Procedure

Capture the current spec before you change anything:

```sh
docker inspect juicefarm-worker --format '{{json .HostConfig}}' > hostconfig.json
docker inspect juicefarm-worker --format '{{range .Config.Env}}{{println .}}{{end}}' \
  | grep -v '^$' > worker.env
chmod 600 worker.env      # JM_META carries the metadata URL
```

Ship the source at the commit you want and build. `Dockerfile.dirinherit`
(alongside this file) rebuilds the Go stage with the SAME flags as stage 1 of
the real Dockerfile — static CGO in particular, because the `juicedata/mount`
runtime ships an older glibc than the bookworm builder and a dynamically-linked
binary will not run:

```sh
git archive --format=tar HEAD | gzip > jm-src.tgz     # on the dev machine
# ... copy to the NAS, unpack, then:
docker build -f Dockerfile.dirinherit -t juicefarm:<tag> .
```

**Verify the binary before you deploy it.** `-s -w` strips symbol names, so
`strings | grep <yourFunc>` proves nothing either way. What does work: run the
affected package's tests with the same toolchain against the same source.

```sh
docker run --rm -v "$PWD":/src -w /src golang:1.26-bookworm \
  go test ./internal/derivatives/ -run TestCreatedDirInherits -count=1 -v
```

Swap the container, keeping the old one as a rollback:

```sh
docker stop -t 60 juicefarm-worker            # expect exit code 0, NOT 137
docker rename juicefarm-worker juicefarm-worker-prev
docker run -d --name juicefarm-worker \
  --restart unless-stopped \
  --network ix-juicemount_default \
  --cap-add CAP_SYS_ADMIN \
  --security-opt apparmor=unconfined \
  --device /dev/fuse \
  -v /mnt/zSSD/juicemount/juicefarm-state:/state \
  -v /mnt/zSSD/juicemount/juicefarm-cache:/jfs-cache \
  --env-file worker.env \
  juicefarm:<tag>
```

**Gate on the mount, not on `docker ps`.** A container that cannot mount still
shows as `Up`:

```sh
for i in $(seq 20); do
  docker exec juicefarm-worker test -d /jfs/.juicemount/derivatives && break
  sleep 3
done
```

If that loop never succeeds, roll back immediately:

```sh
docker rm -f juicefarm-worker
docker rename juicefarm-worker-prev juicefarm-worker
docker start juicefarm-worker
```

---

## Prove the change is live

`docker ps` showing the new tag proves the image, not the behaviour. Run the new
binary against one real asset that has no derivatives yet — prefer something in
`.trash/`, so the write is inconsequential — and observe the result:

```sh
docker exec juicefarm-worker /usr/local/bin/jmfarm \
  -files '/jfs/.trash/.../something.mp4' \
  -mount /jfs -db /state/proof.db -blobs -concurrency 1
docker exec juicefarm-worker ls -ldn /jfs/.juicemount/derivatives/<inode>
```

Use a scratch `-db` so the probe does not enter the live derivative index, and
remove the probe directory and the scratch DB afterwards.

For the ownership fix specifically, the end-to-end proof is on the CLIENT: the
farm creates the directory as root, and the Mac must then be able to write into
it. Confirmed 2026-08-12 with a control — a directory created by the new binary
accepted a client write, and one created before the fix still refused.

---

## Housekeeping

`juicefarm-worker-prev` is the rollback. Leave it until the new build has run
long enough to trust, then `docker rm juicefarm-worker-prev`. Old image tags
(`juicefarm:stream-exclusion`, `:local`, `:pre-jm18*`) are the deeper fallback;
they cost disk, so prune deliberately rather than as a side effect.

## Host-reboot recovery for standalone workers

`unless-stopped` is not sufficient evidence of reboot recovery: a clean Docker
shutdown can leave a standalone container stopped after the daemon returns. For
every render node (and any remotely enrolled worker), install a supervisor
systemd unit after creating the pinned container. It preserves an explicit
operator stop while bringing the worker back after a host reboot or abnormal
exit:

```ini
# /etc/systemd/system/juicefarm-gpu-rc0.5.service
[Unit]
Description=JuiceFarm RC worker (GPU)
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=:/bin/sh -c 'running=$(/usr/bin/docker inspect -f "{{.State.Running}}" juicefarm-gpu-rc0.5) || exit 1; if [ "$running" != true ]; then /usr/bin/docker start juicefarm-gpu-rc0.5 || exit 1; fi; status=$(/usr/bin/docker wait juicefarm-gpu-rc0.5) || exit 1; exit "$status"'
ExecStop=/usr/bin/docker stop --time=30 juicefarm-gpu-rc0.5
Restart=on-failure
RestartSec=15
TimeoutStartSec=0
TimeoutStopSec=60

[Install]
WantedBy=multi-user.target
```

Enable it only after verifying the container's immutable digest and render
admission probe. `ExecStart` can attach to an already running container or
start a stopped one, then passes the container's exit code to systemd. The
`:` prefix leaves shell `$` expansions untouched by systemd. A
normal stop leaves the unit inactive; an abnormal exit triggers
`Restart=on-failure`:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now juicefarm-gpu-rc0.5.service
sudo systemctl is-enabled juicefarm-gpu-rc0.5.service
```

Acceptance must reboot the node (or restart Docker), then verify the worker
heartbeat, exact image digest, live decode/encode/AI probes, and a replacement
runtime ID. A disabled node must remain stopped until explicitly enabled.
