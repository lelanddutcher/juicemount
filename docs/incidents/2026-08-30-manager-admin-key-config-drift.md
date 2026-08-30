# Manager admin-key configuration drift — 2026-08-30

## Summary

A direct Compose recreation of the TrueNAS Manager replaced a healthy container
with one that could not start because `JM_ADMIN_KEY` was empty in the persisted
custom-app configuration. The prior live container had a valid key injected only
in its runtime environment. Its healthy state therefore concealed drift between
the running container and the deployment source of truth.

The Manager correctly failed closed because JuiceMount Link was enabled. The
storage pool, Redis, MinIO, JuiceFS, queue, and source media were not changed by
this incident. The Manager was restored from its captured runtime configuration
and the farm remained paused.

## Root cause

This was a JuiceMount deployment-configuration defect, not a JuiceFS defect.

The committed production Compose template explicitly set `JM_ADMIN_KEY` to an
empty string and documented empty authentication as acceptable for LAN-only use.
At some later point the live deployment enabled Link and injected a valid key
without persisting that value into the TrueNAS custom-app YAML. A normal container
restart continued to reuse the live runtime environment, but `docker compose up`
rendered the empty stored value and discarded the runtime-only credential.

Three gaps allowed the drift to survive:

1. Compose used an empty default instead of required interpolation, so rendering
   succeeded with no key.
2. The update helper reconstructed runtime environment entries as full
   `KEY=value` command arguments and its `--skip-manager` guidance recommended a
   separate direct Compose recreation without an authentication preflight.
3. The Manager rejected an empty key only when Link was enabled; an unedited
   `CHANGEME_*` key could otherwise start in LAN-only mode.

## Corrective changes

- `server/docker-compose.yml` now uses required Compose interpolation for
  `JM_ADMIN_KEY`. `docker compose config --quiet` fails before reconciliation if
  the deployment environment does not supply it.
- `server/scripts/update-server.sh` validates the live Manager's key before it
  builds, stops, or removes either workload container.
- The updater forwards runtime environment values by variable name. Values are
  exported only inside the launch subshell and no longer appear in `docker run`
  argv.
- Manager API verification expands the key only inside the container and streams
  curl header configuration over stdin, keeping the value out of argv.
- Updater recovery snapshots retain container topology but redact every
  environment value; Docker healthchecks also stream authentication over stdin.
- The Manager rejects whitespace-only, short, and common placeholder keys even
  when Link is disabled. A truly unset key remains available only for explicit
  Link-disabled local development.
- TrueNAS and server install documentation now treats the Manager key as durable
  deployment state that must be preserved across every upgrade.
- Tests lock the Compose render-time requirement, binary validation, updater
  preflight ordering, and secret-safe argument construction.

## Deployment rule

Before any Manager recreation, supply the existing 32+ character key through the
deployment environment and require `docker compose config --quiet` to succeed.
Never infer that the persisted app YAML is complete merely because the current
container is healthy. Never copy the secret into a command line, log, repository
file, or incident attachment.

## Upstream disposition

Do not report this incident to JuiceFS. JuiceFS was not on the Manager startup or
Compose configuration path, and no JuiceFS behavior contributed to the dropped
environment value. This belongs entirely to the JuiceMount deployment contract.
