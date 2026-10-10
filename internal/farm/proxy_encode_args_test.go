package farm

import "testing"

func hasArg(args []string, want string) bool {
	for _, arg := range args {
		if arg == want {
			return true
		}
	}
	return false
}

func TestProxyEncodeArgsUsesEncoderFamilySpecificFlags(t *testing.T) {
	cases := []struct {
		vcodec string
		want   []string
		absent []string
	}{
		{
			vcodec: "libx264",
			want:   []string{"-c:v", "libx264", "-pix_fmt", "yuv420p", "-crf", "-preset"},
			absent: []string{"-vaapi_device", "-qp", "-global_quality"},
		},
		{
			vcodec: "h264_vaapi",
			want:   []string{"-vf", "scale_vaapi=format=nv12", "-c:v", "h264_vaapi", "-qp"},
			absent: []string{"-crf", "-preset", "-pix_fmt", "-global_quality"},
		},
		{
			vcodec: "hevc_vaapi",
			want:   []string{"scale_vaapi=format=nv12", "-c:v", "hevc_vaapi", "-qp"},
			absent: []string{"-crf", "-preset"},
		},
		{
			vcodec: "hevc_qsv",
			want:   []string{"-c:v", "hevc_qsv", "-global_quality", "-preset"},
			absent: []string{"-vaapi_device", "-crf"},
		},
		{
			vcodec: "hevc_nvenc",
			want:   []string{"-c:v", "hevc_nvenc", "-cq", "-preset"},
			absent: []string{"-vaapi_device", "-global_quality", "-crf"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.vcodec, func(t *testing.T) {
			args := proxyEncodeArgs(tc.vcodec, 21, "slow")
			for _, want := range tc.want {
				if !hasArg(args, want) {
					t.Errorf("args = %v; missing %q", args, want)
				}
			}
			for _, absent := range tc.absent {
				if hasArg(args, absent) {
					t.Errorf("args = %v; must not contain %q", args, absent)
				}
			}
		})
	}
}

func TestProxyEncodeArgsFamilyDefaults(t *testing.T) {
	for _, vcodec := range []string{"libx264", "h264_vaapi", "hevc_vaapi", "hevc_qsv", "hevc_nvenc"} {
		t.Run(vcodec, func(t *testing.T) {
			if args := proxyEncodeArgs(vcodec, 0, ""); len(args) == 0 {
				t.Fatal("no encoder arguments")
			}
		})
	}
}

func TestProxyDecodeArgsNeverSilentlyUseCPUForHardwareEncoder(t *testing.T) {
	cases := map[string]string{
		"hevc_vaapi": "vaapi",
		"hevc_qsv":   "qsv",
		"hevc_nvenc": "cuda",
	}
	for codec, accelerator := range cases {
		args := proxyDecodeArgs(codec)
		if !hasArg(args, "-hwaccel") || !hasArg(args, accelerator) {
			t.Fatalf("%s decode args = %v, want explicit %s hwaccel", codec, args, accelerator)
		}
	}
	if args := proxyDecodeArgs("libx264"); len(args) != 0 {
		t.Fatalf("CPU fallback decode args = %v, want no hwaccel override", args)
	}
}
