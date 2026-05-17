# Dev setup — passwordless mount/unmount for automated testing

JuiceMount's mount and unmount paths require root privileges
because macOS's `mount_nfs(8)` and `umount(8)` are restricted system
commands. Out of the box, every restart pops a macOS admin password
prompt — fine for end-users (one prompt per session), painful for
the dev workflow where every test cycle restarts the app.

This document sets up **passwordless sudo for mount_nfs and umount**
on your local machine. With it configured:

- JuiceMount starts and mounts non-interactively
- Restarting for testing is free
- End-user behavior on machines WITHOUT this config is unchanged
  (the AppleScript admin prompt path still works as fallback)

## Why this is safe

The sudoers entry grants password-free execution of exactly four
binaries with restricted argument shapes:

- `/sbin/mount_nfs` — mount an NFS volume
- `/sbin/umount` — unmount a volume
- `/bin/mkdir` — create the mount point directory

These commands are individually used by the bridge with arguments
that are constructed in-process from typed config — not from
user-controllable strings. There's no shell expansion, no
indirection through `sh -c`, and no broad `ALL` rule.

A malicious local process could still call `sudo mount_nfs` on its
own — but if a malicious process is running locally, it already has
your user privileges and far worse options.

## One-time setup

Run this in a terminal — it will prompt for your admin password
**once** to create the sudoers file, then never again:

```bash
sudo tee /etc/sudoers.d/juicemount-mount >/dev/null <<EOF
# JuiceMount passwordless mount/unmount.
# See docs/dev-setup.md in the repo for rationale and scope.
%admin ALL=(ALL) NOPASSWD: /sbin/mount_nfs, /sbin/umount, /bin/mkdir
EOF
sudo chmod 0440 /etc/sudoers.d/juicemount-mount
sudo visudo -c -f /etc/sudoers.d/juicemount-mount
```

The final `visudo -c` verifies syntax and refuses to apply a broken
file. If it prints `parsed OK`, you're set.

To scope the rule to your specific user instead of all admins,
replace `%admin` with your username (the output of `whoami`).

## Verifying it works

```bash
# Probe sudo non-interactivity:
sudo -n /sbin/mount_nfs -h >/dev/null 2>&1 && echo "OK" || echo "FAIL"
```

If `OK`, restart JuiceMount and confirm in
`~/Library/Logs/JuiceMount/juicemount.log` that you see:

```
"nfs mounted via passwordless sudo"
```

instead of the previous AppleScript-with-admin prompt path.

## Undoing it

```bash
sudo rm /etc/sudoers.d/juicemount-mount
```

The bridge automatically reverts to the AppleScript admin-prompt
path when passwordless sudo is unavailable. No code change needed
to revert.

## Behavior on machines WITHOUT this config

The bridge probes `sudo -n` before trying. If passwordless sudo
isn't available, it falls through to the AppleScript admin-prompt
path that's been there since v1. End users on shipped JuiceMount
builds will see the password prompt exactly once per session, as
expected.

## What's covered, what's not

**Covered:** the in-app mount, the in-app unmount (clean shutdown),
and the privileged force-unmount escalations.

**Not covered:** anything else that escalates privileges. The
`Reclaim` button uses `tmutil` which doesn't need root. The
`Export Diagnostics` button runs entirely in user space. No other
paths use admin-elevation.
