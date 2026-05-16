# Competitive analysis: Shade

## What it is

Shade (<https://shade.inc>) is an organizational layer for creative
assets. It indexes existing folders on local + cloud storage, runs
on-device CoreML models to auto-tag images and videos by content, and
exposes a Spotlight-style universal search across the user's library.
Reverse image search, color palette extraction, face/object detection.

Architecturally Shade is not a file system — it's a metadata index that
sits *next to* whatever storage the user already has. It complements
JuiceMount rather than competes with it.

## Why this is in tier 6 (not the main path)

Search-and-tag is genuinely valuable for creative teams, but it's the
*least* differentiating thing for a self-hostable file system. Editors
already have:

- macOS Spotlight (free)
- The `mdfind` command line
- Adobe Bridge (free, somewhat integrated)
- Frame.io / Suite review tools (paid)

JuiceMount can be excellent without ever shipping content-based search.
Shade is a tier-6 reference for if-and-when a contributor wants to build
this layer; we don't gate production-readiness on it.

## What to mirror (if and when we hit tier 6)

### 1. CoreML on-device tagging

Shade's tags are generated locally — no upload to the cloud. This is the
right privacy model for creative pros (their footage is sensitive).

**Our adaptation:** `mediainfo` for codec/EXIF/duration. CoreML Vision
framework for object + face detection. CLIP-style embeddings (via Apple's
ImageFeatures API) for "find clips that look like this."

### 2. Spotlight-style universal launcher

Shade has a Cmd-Shift-S quick-search. Type a query; see results from
across the indexed library, with previews and quick actions.

**Our adaptation:** we already have a Cmd-Shift-F search window backed by
FTS5 filename search. Tier-6 extension: same window, but results include
content-tag matches and visual-similarity matches.

### 3. Index lives alongside existing folder structure

Shade doesn't try to replace folders. It indexes them, adds metadata,
exposes a better search.

**Our adaptation:** content tags and embeddings live in JuiceMount's
metadata DB as sidecar fields per file. The file tree is untouched. A
user disabling tier-6 simply loses search-by-content; everything else
keeps working.

## What we explicitly don't take

### 1. Cloud upload for AI processing

Shade has cloud-fallback for heavier models. We never upload user
content; if a tag requires more horsepower than the user's machine has,
we skip it and tell them why.

### 2. Tagging as a separate product

Shade is positioning as the *thing you organize around*. We are not — the
file system is the product. Tagging is an additive feature in a single
preferences pane.

### 3. Cross-library deduplication

Shade does visual-similarity dedup across an entire library. Useful for
photographers; mostly noise for editors (they often need duplicates).
Skip.

## Bottom line

Shade-class search is a "nice to have" that ships when:

1. Tiers 1–5 are production-ready
2. A contributor wants to build it
3. The CoreML approach can run on the user's existing hardware without
   degrading mount performance

Until all three are true, this tier sits.
