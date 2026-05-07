package main

import (
    "context"
    "fmt"
    "os"
    "time"
    "github.com/lelanddutcher/juicemount/internal/proxy"
)

func main() {
    cacheDir, _ := os.MkdirTemp("", "proxy-cache-")
    defer os.RemoveAll(cacheDir)
    fmt.Printf("Cache dir: %s\n", cacheDir)
    m, _ := proxy.NewManager(cacheDir, 1)
    defer m.Stop()

    src := os.Args[1]
    codec := proxy.DetectByExtension(src)
    fmt.Printf("Source: %s (codec: %s)\n", src, codec)

    // Show the args we'd pass to ffmpeg
    spec := proxy.DefaultSpec(src, "/tmp/showargs.mp4", codec)
    fmt.Printf("Encoder spec: width=%d bitrate=%s hardware=%v\n", spec.Width, spec.Bitrate, spec.Hardware)
    
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    start := time.Now()
    r := m.GetBlocking(ctx, src)
    fmt.Printf("\nResult: status=%s codec=%s time=%v\n", r.Status, r.Codec, time.Since(start))
    if r.Err != nil { fmt.Printf("Error: %v\n", r.Err) }
    if info, err := os.Stat(r.ProxyPath); err == nil {
        fmt.Printf("Proxy: %s (%d bytes)\n", r.ProxyPath, info.Size())
    }
    fmt.Printf("Stats: %+v\n", m.Stats())
}
