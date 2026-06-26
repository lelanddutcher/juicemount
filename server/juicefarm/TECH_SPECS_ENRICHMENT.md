# Farm tech-specs enrichment — match OL's deep-specs so the collapsed panel loses nothing

> **STATUS: EARMARKED — farm task, not yet built. (2026-06-26)**
> OL's deep-specs ffprobe panel is a strict SUPERSET of the farm's `tech` (probe.go) except `video.log_format`.
> When the two panels (tech / deep specs) collapse into one, the farm must be the superset or the panel regresses.
> This is FARM under-capture, **not** an OL bug (confirmed). Schema is `additionalProperties:true` on
> `tech`/`video`/`audio[]` → all additive, non-breaking. Coordinated in the contract repo PROVIDER_STATUS (2026-06-26).

## Fields to add to `internal/farm/probe.go` (Tech / VideoTrack / AudioTrack)
**Cheap, already in the current `ffprobe -show_streams -show_format` output (just map them):**
- `video.profile`, `video.level` (e.g. "Main 10" @ L150)
- `video.codec_long_name`, `video.codec_tag` ("hvc1")
- `video.field_order` (interlaced/progressive badge)
- `video.color_range` ("tv"/"pc")
- `video.fps_num` + `video.fps_den` — exact rational ALONGSIDE the rounded `fps` (preserve 24000/1001 vs 23.976).
  **Additive — do NOT repurpose `fps`.**
- `video.sample_aspect_ratio` + `video.display_aspect_ratio` (anamorphic correctness; neither side has it today)
- `video.coded_width`/`coded_height`, `nb_frames`
- `audio[].channel_layout` ("stereo"/"5.1"), `audio[].bit_rate`, `audio[].title`
- `container`: keep first-token "mov" for the facet/index key BUT also emit `format_long_name` ("QuickTime / MOV")
- `timecode`: emit `drop_frame` bool + source enum (tmcd/formatTag/videoStream) to match OL's TimecodeInfo

**Needs a SECOND ffprobe pass (the one real lift):**
- `hdr_metadata` nested object `{mastering_display_luminance_min, mastering_display_luminance_max, max_cll,
  max_fall, dolby_vision_profile}` — requires `Probe()` to add `-show_frames -read_intervals "%+0.1"` and read
  stream+frame `side_data_list`. The current `deriveHDR` only reads transfer/primaries and can **never** emit
  Dolby Vision or luminance. **Without this, collapsing the panels REGRESSES HDR vs today's OL deep panel.**

## Correctness fixes (independent of the field adds)
- **Stop picking `attached_pic` cover-art as the video stream** — farm's "first video stream wins" picks album
  art on MP3/M4A; exclude `disposition.attached_pic`. (OL already excludes it.)
- Add per-track `stream_index` + `is_default` (`disposition.default`).

## Future-proofing rules (so adding fields later never breaks OL)
- Every new field OPTIONAL — never add to `required`. Additive-only — never rename/retype existing fields.
- Bump the top-level schema `version` integer when the farm starts populating new fields.
- Group HDR side-data under nested `hdr_metadata` (`additionalProperties:true`) so it can grow (HDR10+).
- Keep the Go nil-slice→`[]` discipline (init `Audio` to `[]AudioTrack{}`) so empty serializes as `[]` not
  `null` (Swift Codable decode-abort). See [[feedback_go_swift_json_null]].

## Related
- **Audio silent-data-loss is FIXED** (commit c75df39, `internal/farm/audio.go`) but the deployed farm still
  has the old code → **rebuild + re-sweep multi-mic clips** (their transcripts/waveforms are dead silence).
- Cross-link: contract repo `PROVIDER_STATUS.md` (2026-06-26 section), `metadata.schema.json`.
