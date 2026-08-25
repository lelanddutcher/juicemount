package farm

import (
	"strings"
	"testing"
)

// The three encoder families need genuinely different ffmpeg wiring; a wrong
// flag here is a hard ffmpeg error at sweep time, not a soft quality change.
func TestProxyEncodeArgs(t *testing.T) {
	cases := []struct {
		vcodec   string
		wantSub  []string // must all be present, in order
		notWant  []string // must NOT be present
	}{
		{
			vcodec:  "libx264",
			wantSub: []string{"-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "-preset"},
			notWant: []string{"-vaapi_device", "-qp"},
		},
		{
			vcodec:  "libx265",
			wantSub: []string{"-c:v", "libx265", "-pix_fmt", "yuv420p", "-crf"},
			notWant: []string{"-vaapi_device"},
		},
		{
			vcodec:  "h264_vaapi",
			wantSub: []string{"-vaapi_device", "/dev/dri/renderD128", "format=nv12,hwupload", "-c:v", "h264_vaapi", "-qp"},
			notWant: []string{"-preset", "-crf"}, // VAAPI rejects both → hard error
		},
		{
			vcodec:  "hevc_vaapi",
			wantSub: []string{"-vaapi_device", "-c:v", "hevc_vaapi", "-qp"},
			notWant: []string{"-preset"},
		},
		{
			vcodec:  "hevc_nvenc",
			wantSub: []string{"-c:v", "hevc_nvenc", "-crf"},
			notWant: []string{"-vaapi_device"},
		},
		{
			vcodec:  "hevc_qsv",
			wantSub: []string{"-c:v", "hevc_qsv", "-global_quality"},
			notWant: []string{"-vaapi_device", "-crf"},
		},
	}
	for _, tc := range cases {
		args := proxyEncodeArgs(tc.vcodec, 21, "slow")
		got := strings.Join(args, "\x00") // join with a separator that can't appear in a flag
		for _, w := range tc.wantSub {
			if !strings.Contains(got, "\x00"+w+"\x00") && !strings.HasSuffix(got, w) && !strings.HasPrefix(got, w) &&
				!strings.Contains(got, "\x00"+w) {
				t.Errorf("%s: want arg %q in %v", tc.vcodec, w, args)
			}
		}
		for _, n := range tc.notWant {
			if strings.Contains(got, "\x00"+n+"\x00") || strings.Contains(got, "\x00"+n+",") ||
				strings.Contains(got, n+",hwupload") || got == n {
				t.Errorf("%s: must not contain %q (got %v)", tc.vcodec, n, args)
			}
		}
	}
}

func TestProxyEncodeArgsDefaults(t *testing.T) {
	// crf<=0 falls back to family-appropriate defaults without panicking.
	for _, v := range []string{"libx264", "libx265", "h264_vaapi", "hevc_vaapi", "hevc_qsv", "hevc_nvenc"} {
		if args := proxyEncodeArgs(v, 0, ""); len(args) == 0 {
			t.Errorf("%s: no args", v)
		}
	}
}
