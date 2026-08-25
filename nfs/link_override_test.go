package nfs

// T2.2: LinkEndpointOverride swaps NAS LAN endpoints to the NAS tailnet
// address once the Link node is up. Scheme/port/userinfo/path must survive;
// anything unparseable passes through untouched so a bad config string can
// never turn into a silently different endpoint.

import "testing"

func TestLinkEndpointOverride(t *testing.T) {
	const nas = "100.64.0.1"
	cases := []struct {
		name            string
		nas, redis, min string
		wantRedis       string
		wantMin         string
	}{
		{
			name:      "redis url with userinfo, port, db",
			nas:       nas,
			redis:     "redis://:secret@192.168.0.197:30179/1",
			wantRedis: "redis://:secret@100.64.0.1:30179/1",
			min:       "", wantMin: "",
		},
		{
			name:      "minio http endpoint",
			nas:       nas,
			redis:     "redis://192.168.0.197:30179/1",
			wantRedis: "redis://100.64.0.1:30179/1",
			min:       "http://192.168.0.197:30151",
			wantMin:   "http://100.64.0.1:30151",
		},
		{
			name:      "bare host:port form",
			nas:       nas,
			redis:     "192.168.0.197:30179",
			wantRedis: "100.64.0.1:30179",
			min:       "192.168.0.197:9000",
			wantMin:   "100.64.0.1:9000",
		},
		{
			name:      "no port on either side",
			nas:       nas,
			redis:     "redis://nas.example/1",
			wantRedis: "redis://100.64.0.1/1",
			min:       "http://nas.example/bucket",
			wantMin:   "http://100.64.0.1/bucket",
		},
		{
			name:      "empty nas addr is passthrough",
			nas:       "",
			redis:     "redis://192.168.0.197:30179/1",
			wantRedis: "redis://192.168.0.197:30179/1",
			min:       "http://192.168.0.197:30151",
			wantMin:   "http://192.168.0.197:30151",
		},
		{
			name:      "unparseable garbage passthrough",
			nas:       nas,
			redis:     "://nope",
			wantRedis: "://nope",
			min:       "://nope",
			wantMin:   "://nope",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotRedis, gotMin := LinkEndpointOverride(c.nas, c.redis, c.min)
			if gotRedis != c.wantRedis {
				t.Errorf("redis = %q, want %q", gotRedis, c.wantRedis)
			}
			if gotMin != c.wantMin {
				t.Errorf("minio = %q, want %q", gotMin, c.wantMin)
			}
		})
	}
}
