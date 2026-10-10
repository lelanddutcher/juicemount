# minio-keyd file protocol

This is the contract between the Manager and `minio-keyd` (`keyd.sh` in
this directory). keyd creates and removes the per-Mac MinIO service
accounts (`mac-<12 hex>-<gen>`, parent `jm-macs`). The Manager never holds
a MinIO admin credential. The two talk only through files in the shared
`objkeys` directory. keyd reads it every 2 seconds.

| File | Writer | Reader | Holds |
|---|---|---|---|
| `desired.json` | Manager | keyd | The keys that must exist. No secrets. |
| `results/<name>.json` | keyd | Manager | The secret of one key keyd just created. |
| `applied.json` | keyd | Manager | What MinIO reports for each key, and the last error. |
| `keyd-created-seq`, `.keyd-mc` | keyd | keyd | keyd's own state. The Manager must not touch them. |

Every writer replaces a file atomically (write a temporary file, then
rename it). Files are mode 0600 in a 0700 directory.

## desired.json

Write it with Go's `json.Marshal` (or `json.MarshalIndent`) of this shape:

```go
type Desired struct {
    Seq             uint64       `json:"seq"`
    AllowMassDelete uint64       `json:"allow_mass_delete,omitempty"`
    Keys            []DesiredKey `json:"keys"`
    Remove          []string     `json:"remove,omitempty"` // explicitly authorized names
}
type DesiredKey struct {
    Name        string `json:"name"`  // mac-<12 lowercase hex>-<1-999>
    State       string `json:"state"` // always "active"
    NeedsSecret bool   `json:"needs_secret,omitempty"`
}
```

keyd reads only these fields: `seq`, `allow_mass_delete`, `keys`, `remove`, and per
key `name`, `state`, `needs_secret`. An optional `parent` (top level or per
key) must be `"jm-macs"`.

Other members are allowed and ignored if their value is a string, number,
`true`, `false` or `null`, including strings that Go escapes (`&`, `<`,
`>`, quotes) and UTF-8. Objects or arrays anywhere except `keys` and the
named `remove` array are refused.

`"keys": null` (a nil Go slice) means an empty list. The mass-delete guard
still applies to it.

keyd refuses the whole file, and changes nothing, when:

- it is not strict JSON: a syntax error, a member twice in one object, data
  after the closing brace, a NUL byte, or a raw control character (including
  DEL) in a string;
- a member name has an escape, is not 1-64 of `A-Z a-z 0-9 _`, or differs
  only in case from a field keyd reads (`Seq`, `STATE`): a Go decoder
  matches member names without regard to case;
- a field keyd reads has an escape sequence in its value;
- `seq` or `allow_mass_delete` is not a whole number of at most 15 digits,
  or `seq` is missing, or there is no `keys` member;
- a name does not match, appears twice, or has a state other than
  `active`; a removal name is invalid, duplicated, or also wanted; or a parent is not `jm-macs`;
- `seq` is lower than the `seq` in `applied.json`.

The refusal reason is in `applied.json` `error`, prefixed
`refused desired.json:`.

A missing `desired.json` means "no instruction". It never removes keys.

## seq

- Raise `seq` on every change you want keyd to act on.
- Never write a lower `seq` than the one in `applied.json`.
- Removing more than 3 keys, or more than 1 key and more than 25% of the
  keys, in one step needs `allow_mass_delete` equal to `seq`.
- `remove` names each key whose deletion the Manager authorized through a
  revoke, retirement, or confirmed orphan removal. keyd deletes only names
  in this array that MinIO currently lists under `jm-macs`. A managed key
  absent from `keys` but absent from `remove` is preserved and reported in
  `applied.json` so the Manager can show it as an orphan. This holds even
  when another named removal is authorized or `allow_mass_delete` is set.
  An old desired document without `remove` grants no deletion authority.

## Secrets and needs_secret

1. keyd creates a key that is in `desired.json` but not in MinIO. MinIO
   makes the secret. keyd writes it to `results/<name>.json`:
   `{"name":"...","secret_key":"...","created_ms":N}`.
2. The Manager stores the secret durably.
3. If `desired.json` has `needs_secret` for that key, the Manager writes
   `desired.json` without it.
4. Only then the Manager deletes `results/<name>.json`.

Step 3 before step 4 is required.

If the Manager has no secret for a key that exists (for example the result
file was deleted before the secret was stored), it asks for a new one: set `"needs_secret": true` for the key **and raise `seq`** in the
same write. keyd removes the key and creates it again, once per `seq`. It
does not do this while `results/<name>.json` still exists.

keyd also guards against a Manager that skips step 3. If keyd sees that the
result was taken while `needs_secret` was still set, it ignores
`needs_secret` for that key until it reads a `desired.json` without it.
`applied.json` `error` then says `needs_secret for <name> ignored`. Without
this guard, any later `seq` raise (a revoke of another Mac) would re-create
the key and kill the secret the Manager just stored. keyd cannot tell a stale
flag from a new request by itself. So after a delivery, a new request must
come after a `desired.json` without the flag that keyd has applied:
`applied.json` `seq` at least the `seq` of that write.

## applied.json

```json
{"seq":N,"verified_ms":N,"error":"...","keys":[
  {"name":"...","exists":true,"status":"on","parent":"jm-macs","implied_policy":true}]}
```

keyd builds it from what MinIO reports (`svcacct ls`, `svcacct info`), not
from `desired.json`. `seq` is the last `desired.json` seq keyd applied. A key
is ready for a Mac only when `exists` is true and `parent` is `jm-macs`.

## Stopping keyd

A stop (`docker stop`, TERM) during a key create waits for MinIO's reply, so
the secret reaches `results/`. The wait is at most 50 seconds
(`JM_KEYD_HOLD_MAX`). After that, keyd kills the stuck call, removes the key
if MinIO made it (5 seconds at most), logs `GAVE UP`, and exits. The compose
service has `stop_grace_period: 60s`, so docker does not kill keyd first. If
the key survives a give-up, the Manager sees it in `applied.json` with no
secret, and asks again with `needs_secret` and a raised `seq`.
