# Farm queue rollout (JM-16) — TIER 2 PREP, staged not deployed

Flip the server-side `juicefarm` worker from a periodic 900s `-root` sweep to the
**standing queue-drain worker** (`jmfarm -queue`) so Manager `POST /api/farm/sweep`
enqueues are picked up in near-real-time — the behavior OpenLoupe expects.

The code change is one env var (`JM_FARM_QUEUE=1`) on the `juicefarm` service in
`server/docker-compose.yml`. `JM_FARM_INTERVAL: "900"` is kept as the periodic
fallback for when queue mode is unset/0. With `JM_FARM_QUEUE=1` the entrypoint
exec's `jmfarm -queue` and `JM_FARM_INTERVAL` is NOT used.

## What changed in-repo

`server/docker-compose.yml`, `juicefarm` service env:

```yaml
JM_FARM_QUEUE: "1"        # standing queue worker (heartbeat + BRPOP juicefarm: queue)
JM_FARM_INTERVAL: "900"   # fallback only (periodic -root sweep when queue mode off)
```

No change to the `juicemount-manager` service — it already serves
`POST /api/farm/sweep` + `GET /api/farm/jobs` (admin-key). Only the worker needs to
flip to queue mode for those enqueues to drain in real time.

## Deploy steps (manager-deploy convention — do NOT rsync trailing-slash flatten)

> Per the manager farmtab deploy convention: edit the **rendered** compose on the
> TrueNAS host in place, restart ONLY the changed service, verify via `curl` from
> the host (Claude preview can't reach the LAN). Do NOT `rsync` a directory with a
> trailing slash — it flattens the tree and rebuilds a stale image.

Host: `root@192.168.0.197` · key: `codex_truenas_tmp` · app: `ix-juicemount`.

1. SSH in:
   ```
   ssh -i ~/.ssh/codex_truenas_tmp root@192.168.0.197
   ```

2. Locate the rendered compose for the deployed app (the ix-applications
   docker-compose the stack actually runs — NOT this repo's template):
   ```
   find /mnt -path '*ix-juicemount*docker-compose.yml' 2>/dev/null
   ```

3. Edit the rendered compose in place. Under the `juicefarm` service `environment:`
   block, add `JM_FARM_QUEUE: "1"` and KEEP `JM_FARM_INTERVAL: "900"` as the
   fallback. (Match the indentation of the surrounding env keys exactly.)

4. Restart ONLY the worker — do not touch redis/minio/juicefs/manager:
   ```
   docker compose -f <rendered-compose-path> up -d --no-deps juicefarm
   ```
   (If the deployed worker is the additive `docker run --name juicefarm` container
   rather than a compose service, stop/rm just that container and re-run it with the
   added `-e JM_FARM_QUEUE=1`, keeping `-e JM_FARM_INTERVAL=900`. Do not restart any
   other container.)

5. Confirm the worker is in queue mode (look for the queue-mode log line):
   ```
   docker logs --tail 20 juicefarm | grep -i 'queue mode'
   # expect: [juicefarm] queue mode: draining redis://redis:6379/1 ...
   ```

## Verify (curl from the TrueNAS host — preview can't reach the LAN)

Run on `192.168.0.197` (admin-key gated; substitute the manager admin key):

1. Worker liveness + queue depth — `available:true` once the worker heartbeats:
   ```
   curl -fsS -H "X-Admin-Key: $JM_ADMIN_KEY" \
     http://127.0.0.1:30190/api/farm/jobs | jq '{available, queue_depth, workers}'
   ```

2. Enqueue a scoped sweep and confirm it transitions queued → running → done
   (real-time, NOT after a 900s interval):
   ```
   curl -fsS -X POST -H "X-Admin-Key: $JM_ADMIN_KEY" \
     -H 'Content-Type: application/json' \
     -d '{"path":"<small-test-subpath>","kinds":["derivatives"]}' \
     http://127.0.0.1:30190/api/farm/jobs/../sweep
   # then re-poll /api/farm/jobs and watch the job's status advance within seconds
   ```
   (Use the literal `http://127.0.0.1:30190/api/farm/sweep` for the POST; the path
   above is only to show it's the same host:port.)

## Rollback

Remove `JM_FARM_QUEUE` (or set `"0"`) on the `juicefarm` service in the rendered
compose and `up -d --no-deps juicefarm`. The worker reverts to the periodic
`JM_FARM_INTERVAL` sweep — the exact pre-JM-16 behavior. No other service touched.

## Route-name note (canonical, no alias added)

OpenLoupe's `BACKLOG.md → OL-FARM-2` asks for `POST /farm/queue` + `GET /farm/jobs`.
Those are **Mac control-plane** routes (a thin JM-13-authed proxy on the Mac that
LPUSHes to Redis), NOT Manager routes — see `juicemount-contract/FARM_CONTROL_SURFACE.md`
and `FARM_QUEUE_PROTOCOL.md` §"How OpenLoupe reaches the queue". The Manager
deliberately serves the LAN-only, admin-key `POST /api/farm/sweep` + `GET /api/farm/jobs`.

Decision: **document the canonical names; do NOT add a `/farm/queue` alias on the
Manager.** Aliasing would conflate two distinct surfaces (LAN admin-key Manager vs.
Mac control-plane consumer proxy) and is higher-risk. The `/farm/queue` proxy is the
explicitly-deferred OL-FARM-2 (Phase 2) work and belongs on the Mac control plane
(`bridge/cbridge.go`), not in this farm/manager rollout.
