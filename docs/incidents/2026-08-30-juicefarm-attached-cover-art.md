# JuiceFarm audio cover-art proxy failures — 2026-08-30

## Summary

Release-candidate validation found two audio-only assets routed into the video
proxy lane. Both failed in the MP4 encoder. This was a JuiceMount media
classification defect, not a JuiceFS, Redis, MinIO, VAAPI, or storage-capacity
failure.

No source paths, filenames, or credentials are included in this report.

## Impact

- Two one-file CPU proxy jobs ended in the failed state.
- The failures were bounded to the individual assets; no directory-sized retry
  or unrelated CPU fallback was created.
- No corrupt proxy was registered and no active claim or lease was lost.

## Root cause

`ffprobe` represents embedded album artwork as a stream whose `codec_type` is
`video` and whose `disposition.attached_pic` value is `1`. JuiceMount's loose
ffprobe model did not deserialize the disposition object. Its mapper therefore
selected the first `video` stream even when that stream was only cover art.

The proxy generator saw a non-null video track and launched an MP4/H.264 encode
for an audio asset. The resulting mux failure was correct; the preceding
eligibility decision was not.

## Correction

- The ffprobe model now reads `disposition.attached_pic`.
- Attached-picture streams are excluded from motion-video classification.
- If cover art precedes a genuine video stream, the first genuine stream is
  selected.
- Proxy and preview planners re-probe current source bytes for eligibility
  instead of trusting cached tech metadata produced before classifier fixes.
- Regression tests cover audio plus cover art, cover art followed by genuine
  video, and proof that the proxy encoder is not invoked for the audio case.

## Upstream disposition

No upstream issue should be filed. `ffprobe` reported the stream disposition
needed to distinguish artwork from motion video; JuiceMount discarded that
field. This was entirely our model and routing error.

An upstream report would be appropriate only if a supported ffprobe build omits
or contradicts the attached-picture disposition for a reproducible valid media
sample. Any such report must use a synthetic/redacted sample and must not include
production media paths or storage credentials.
