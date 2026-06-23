<!--
One theme per PR. Stability fixes don't share a commit with features.
For security issues, don't open a PR in the open — see SECURITY.md.
-->

## What this changes

<!-- A sentence or two: what does this PR do, and why? -->

## Type

- [ ] Bug / stability fix
- [ ] Feature
- [ ] Docs
- [ ] Refactor / cleanup

## How it was tested

<!--
Real-hardware testing is the gold standard here. Note what you ran:
Finder / Resolve / Premiere workload, NAS vendor, network shape.
-->

- [ ] `go vet ./...` passes
- [ ] `go test -race ./<touched-package>/...` passes
- [ ] Tested against a real mount (not just unit tests) — describe below

## Checklist

- [ ] One theme — no unrelated changes riding along
- [ ] No telemetry / phone-home added
- [ ] No FileProviderExtension introduced (the build script enforces this)
- [ ] Request-path / data-safety changes include a test for the failure being fixed
- [ ] Matches the style of the surrounding code
