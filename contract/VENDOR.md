# Vendored contract (pinned copy)

This directory is a **pinned, read-only copy** of the [`juicemount-contract`](https://github.com/lelanddutcher/juicemount-contract)
repo — the language-neutral wire contract between JuiceMount and OpenLoupe.

- **Source:** `github.com/lelanddutcher/juicemount-contract`
- **Pinned commit:** `357cb9dc02a2f80153deda6def385de19147a6d6`
- **contract_version:** see [`VERSION`](VERSION) (currently `1`)

Only `spec/` (schemas) + `fixtures/` (golden responses) + `VERSION` are vendored — the
files the Go conformance test reads. We vendor a copy rather than a git submodule because
JuiceMount is a **public** repo and the contract repo is private; a private submodule would
break `git clone --recursive` and CI for the public.

## Re-vendoring after a contract bump

```sh
SRC=/path/to/juicemount-contract
rm -rf contract/spec contract/fixtures
cp -R "$SRC/spec" "$SRC/fixtures" contract/
cp "$SRC/VERSION" contract/VERSION
# update the pinned commit above; the conformance test will fail loudly if the
# real control plane no longer matches the new fixtures.
```

The conformance test (`bridge/contract_conformance_test.go`) reads `contract/spec/` +
`contract/fixtures/` directly, so a re-vendor immediately re-checks the live server against
the new contract.
