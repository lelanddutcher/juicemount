import Foundation
import AVFoundation
import CryptoKit
import ImageIO

// Compiled alongside the consumer's unmodified production client sources by
// scripts/build-consumer-contract-probe.sh. The Go acceptance test supplies an
// isolated loopback server backed by real JuiceMount handlers and temporary data.
@main
struct ConsumerContractProbe {
    struct ThumbnailSlot: Decodable {
        let inode: UInt64
        let sourceHash: String
        let sourceSize: Int64
        let sourceMtime: Int64
        let displayPath: String
        let jpegSHA256: String
        let jpegSize: Int64
    }
    struct ThumbnailFixture: Decodable {
        let hot: ThumbnailSlot
        let stale: ThumbnailSlot
        let cold: ThumbnailSlot
    }

    static func thumbnailSlot() throws -> ThumbnailSlot {
        guard let path = ProcessInfo.processInfo.environment["JM_CONSUMER_MEDIA_FIXTURE"], path.hasPrefix("/"),
              let bytes = FileManager.default.contents(atPath: path),
              let name = ProcessInfo.processInfo.environment["JM_CONSUMER_CACHE_SLOT"] else {
            throw ProbeError.failed("missing owned thumbnail descriptor or slot")
        }
        let f = try JSONDecoder().decode(ThumbnailFixture.self, from: bytes)
        let slot: ThumbnailSlot
        switch name {
        case "hot": slot = f.hot
        case "stale": slot = f.stale
        case "cold": slot = f.cold
        default: throw ProbeError.failed("unknown thumbnail slot")
        }
        try require(slot.inode > 0 && slot.inode <= UInt64(Int64.max) && slot.sourceHash.count == 16 &&
                    slot.jpegSHA256.count == 64 && slot.jpegSize > 0 && slot.displayPath.hasPrefix("/Volumes/zpool/"),
                    "invalid source-bound thumbnail slot")
        return slot
    }

    static func thumbnail(_ client: JuiceMountClient, positive: Bool) async throws {
        let slot = try thumbnailSlot()
        let inode = Int64(slot.inode)
        let who = await client.whoami()
        try require(who?.contractIsCompatible == true && who?.supports(.lookup, .derivatives, .blob) == true,
                    "production thumbnail capabilities absent")
        let lookup = await client.lookup(path: slot.displayPath)
        try require(lookup?.exists == true && lookup?.inode == inode, "persisted thumbnail mirror identity absent")
        let manifest = await client.derivatives(inode: inode)
        try require(manifest?.exists == true && manifest?.sourceHash == slot.sourceHash &&
                    (positive ? manifest?.isReadyAndFresh("thumbnail") == true : true),
                    "persisted thumbnail row absent")
        guard let row = manifest?.entry("thumbnail") else { throw ProbeError.failed("thumbnail row missing") }
        try require(row.status == "ready" && row.hash == slot.sourceHash && row.sourceSize == slot.sourceSize &&
                    row.sourceMtime == slot.sourceMtime && row.blobRelPath == "poster.jpg" &&
                    row.mediaType == "image/jpeg", "thumbnail row lost source binding")
        let data = await client.blob(inode: inode, kind: "thumbnail")
        if positive {
            guard let data else { throw ProbeError.failed("production thumbnail client returned nil") }
            try require(Int64(data.count) == slot.jpegSize && sha256(data) == slot.jpegSHA256,
                        "thumbnail client bytes differ from real farm poster")
            guard let source = CGImageSourceCreateWithData(data as CFData, nil),
                  let image = CGImageSourceCreateImageAtIndex(source, 0, nil) else {
                throw ProbeError.failed("ImageIO could not decode thumbnail JPEG")
            }
            try require(image.width == 320 && image.height == 180, "thumbnail JPEG dimensions changed")
            print("PASS thumbnail-positive: unmodified client decoded real farm JPEG through provider")
        } else {
            try require(data == nil, "production thumbnail client accepted guarded negative blob")
            print("PASS thumbnail-negative: unmodified client returned nil for guarded blob")
        }
    }
    struct MediaFixture: Decodable {
        let inode: UInt64
        let sourceHash: String
        let sourceSize: Int64
        let sourceMtime: Int64
        let displayPath: String
        let mount: String
        let proxySHA256: String
        let proxySize: Int64
        let codecString: String
    }

    static func require(_ condition: Bool, _ message: String) throws {
        if !condition { throw ProbeError.failed(message) }
    }

    enum ProbeError: Error { case failed(String) }

    static func mediaFixture() throws -> MediaFixture {
        guard let path = ProcessInfo.processInfo.environment["JM_CONSUMER_MEDIA_FIXTURE"],
              path.hasPrefix("/"), let data = FileManager.default.contents(atPath: path) else {
            throw ProbeError.failed("missing owned media descriptor")
        }
        let value = try JSONDecoder().decode(MediaFixture.self, from: data)
        try require(value.inode > 0 && value.inode <= UInt64(Int64.max) && value.sourceSize == 223153 &&
                    value.proxySize > 2048 && value.sourceHash.count == 16 && value.proxySHA256.count == 64 &&
                    value.codecString == "avc1.64000c, mp4a.40.2" && value.mount.hasPrefix("/"),
                    "invalid source-bound media descriptor")
        return value
    }

    static func sha256(_ data: Data) -> String {
        SHA256.hash(data: data).map { String(format: "%02x", $0) }.joined()
    }

    static func proxyMedia(_ client: JuiceMountClient) async throws {
        let f = try mediaFixture()
        let inode = Int64(f.inode)
        let who = await client.whoami()
        try require(who?.contractIsCompatible == true &&
                    who?.supports(.lookup, .derivatives, .changes, .blob) == true,
                    "production media capabilities absent")
        let lookup = await client.lookup(path: f.displayPath)
        try require(lookup?.exists == true && lookup?.inode == inode && lookup?.size == f.sourceSize &&
                    lookup?.mtime == f.sourceMtime && lookup?.nasRelPath == "Project_Foo/consumer-media.mov",
                    "production lookup lost source identity")
        let manifest = await client.derivatives(inode: inode)
        try require(manifest?.exists == true && manifest?.inode == inode && manifest?.sourceHash == f.sourceHash &&
                    manifest?.isReadyAndFresh("proxy") == true, "production manifest rejected sidecar proxy")
        guard let manifest, let row = manifest.entry("proxy") else { throw ProbeError.failed("ready proxy row absent") }
        try require(row.producer == "linux-farm" && row.status == "ready" && row.hash == f.sourceHash &&
                    row.sourceSize == f.sourceSize && row.sourceMtime == f.sourceMtime &&
                    row.blobRelPath == "proxy.mp4" && row.mediaType == "video/mp4" &&
                    row.codec == "h264" && row.codecString == f.codecString && row.blobSize == f.proxySize,
                    "production proxy metadata/source binding changed")
        let changes = await client.derivativeChanges(since: 0)
        try require(changes?.contains(where: { $0.inode == inode && $0.kind == "proxy" &&
                                              $0.status == "ready" && $0.hash == f.sourceHash && $0.updatedAt > 0 }) == true,
                    "production changes decoder lost proxy row")
        let forbidden = await client.blob(inode: inode, kind: "proxy")
        try require(forbidden == nil, "whole-object proxy client policy widened")
        guard let local = JuiceMountBlob.readyURL(manifest: manifest, mountPoint: f.mount, kind: "proxy") else {
            throw ProbeError.failed("production mounted-blob resolver rejected ready proxy")
        }
        let localBytes = try Data(contentsOf: local)
        try require(Int64(localBytes.count) == f.proxySize && sha256(localBytes) == f.proxySHA256,
                    "resolved local proxy bytes do not match byte-bound descriptor")
        let asset = AVURLAsset(url: local)
        try require(try await asset.load(.isPlayable), "AVFoundation says proxy is not playable")
        let tracks = try await asset.loadTracks(withMediaType: .video)
        try require(!tracks.isEmpty, "AVFoundation found no proxy video track")
        let image = try AVAssetImageGenerator(asset: asset).copyCGImage(
            at: CMTime(seconds: 0.25, preferredTimescale: 600), actualTime: nil)
        try require(image.width == 320 && image.height == 180, "AVFoundation could not decode the first video frame")
        print("PASS proxy-media: production lookup, on-miss manifest, changes, resolved local blob, AVFoundation frame; client proxy blob GET forbidden")
    }

    static func proxyRanges(_ base: URL) async throws {
        let f = try mediaFixture()
        let inode = Int64(f.inode)
        guard let blobURL = URL(string: "/blob?inode=\(inode)&kind=proxy", relativeTo: base)?.absoluteURL else {
            throw ProbeError.failed("invalid isolated blob URL")
        }
        let session = URLSession(configuration: {
            let cfg = URLSessionConfiguration.ephemeral
            cfg.timeoutIntervalForRequest = 3
            cfg.timeoutIntervalForResource = 12
            return cfg
        }())
        defer { session.invalidateAndCancel() }
        let mounted = JuiceMountBlob.url(mountPoint: f.mount, inode: inode, relPath: "proxy.mp4")
        let full = try Data(contentsOf: mounted)
        try require(Int64(full.count) == f.proxySize && sha256(full) == f.proxySHA256,
                    "raw transport local reference bytes changed")
        let n = full.count
        let middle = n / 2
        for (range, start, end) in [
            ("bytes=0-1023", 0, 1024),
            ("bytes=\(middle)-\(middle + 1023)", middle, middle + 1024),
            ("bytes=\(n - 1024)-", n - 1024, n)
        ] {
            var request = URLRequest(url: blobURL)
            request.setValue(range, forHTTPHeaderField: "Range")
            let (data, response) = try await session.data(for: request)
            guard let http = response as? HTTPURLResponse else { throw ProbeError.failed("range had no HTTP response") }
            try require(http.statusCode == 206 && http.value(forHTTPHeaderField: "Accept-Ranges") == "bytes" &&
                        http.value(forHTTPHeaderField: "Content-Type")?.hasPrefix("video/mp4") == true &&
                        http.value(forHTTPHeaderField: "Content-Range") == "bytes \(start)-\(end - 1)/\(n)" &&
                        http.value(forHTTPHeaderField: "Content-Length") == String(end - start) &&
                        http.value(forHTTPHeaderField: "X-JM-Kind") == "proxy" &&
                        http.value(forHTTPHeaderField: "X-JM-Hash") == f.sourceHash &&
                        http.value(forHTTPHeaderField: "X-JM-Source-Size") == String(f.sourceSize) &&
                        http.value(forHTTPHeaderField: "X-JM-Source-Mtime") == String(f.sourceMtime) &&
                        data == Data(full[start..<end]), "range \(range) failed headers or exact bytes")
        }
        var invalid = URLRequest(url: blobURL)
        invalid.setValue("bytes=\(n)-", forHTTPHeaderField: "Range")
        let (_, invalidResponse) = try await session.data(for: invalid)
        guard let invalidHTTP = invalidResponse as? HTTPURLResponse else { throw ProbeError.failed("416 had no HTTP response") }
        try require(invalidHTTP.statusCode == 416 &&
                    invalidHTTP.value(forHTTPHeaderField: "Content-Range") == "bytes */\(n)",
                    "unsatisfiable range was not rejected")
        let (whole, wholeResponse) = try await session.data(from: blobURL)
        guard let wholeHTTP = wholeResponse as? HTTPURLResponse else { throw ProbeError.failed("full GET had no HTTP response") }
        try require(wholeHTTP.statusCode == 200 && wholeHTTP.value(forHTTPHeaderField: "Content-Length") == String(n) &&
                    wholeHTTP.value(forHTTPHeaderField: "Content-Type")?.hasPrefix("video/mp4") == true &&
                    sha256(whole) == f.proxySHA256 && whole == full, "full raw provider GET differs from local bytes")
        print("PASS proxy-ranges: three exact ranges, 416, and full provider GET; raw URLSession transport")
    }

    static func main() async {
        do {
            try await run()
        } catch {
            print("FAIL consumer: \(error)")
            exit(1)
        }
    }

    static func run() async throws {
        guard CommandLine.arguments.count == 3,
              let url = URL(string: CommandLine.arguments[1]),
              url.scheme == "http", url.host == "127.0.0.1" else {
            throw ProbeError.failed("expected an isolated loopback URL and a known acceptance mode")
        }
        let client = JuiceMountClient(baseURL: url, timeout: 2)
        switch CommandLine.arguments[2] {
        case "thumbnail-positive":
            try await thumbnail(client, positive: true)
        case "thumbnail-negative":
            try await thumbnail(client, positive: false)
        case "proxy-media":
            try await proxyMedia(client)
        case "proxy-ranges":
            try await proxyRanges(url)
        case "reads":
            let who = await client.whoami()
            try require(who?.contractVersion == 3, "whoami did not advertise contract 3")
            try require(who?.supports(.lookup, .residency, .derivatives, .metadata, .pin, .unpin) == true,
                        "required capabilities absent")
            let path = "/Volumes/zpool/Project_Foo/clip_031.mov"
            let lookup = await client.lookup(path: path)
            try require(lookup?.inode == 1180417 && lookup?.exists == true, "lookup identity lost")
            let resident = await client.residency(path: path)
            try require(resident?.resident == true && resident?.total == 880000000,
                        "resident bytes misreported")
            let missing = await client.lookup(path: "/Volumes/zpool/Project_Foo/not_here.mov")
            try require(missing?.exists == false, "missing file not distinguished")
            let derivatives = await client.derivatives(inode: 1180417)
            try require(derivatives != nil, "derivative manifest rejected")
            let metadata = await client.metadata(inode: 1180417)
            try require(metadata != nil, "technical metadata rejected")
            let cache = await client.cacheStatus()
            try require(cache != nil, "cache status rejected")
            let spool = await client.spool()
            try require(spool?.enabled == false, "disabled spool 503 body rejected")
            print("PASS reads: 8 production client calls")
        case "pin-unpin":
            let root = "/Volumes/zpool/consumer-fixture"
            let pinned = await client.pin(path: root)
            try require(pinned?.ok == true, "production consumer pin was rejected")
            var found = false
            for _ in 0..<100 {
                if let status = await client.cacheStatus(),
                   status.scanning.isEmpty,
                   status.roots?.contains(where: { $0.root == root && $0.totalFiles == 1 }) == true {
                    found = true
                    break
                }
                try await Task.sleep(for: .milliseconds(20))
            }
            try require(found, "pin never registered its temporary file")
            let unpinned = await client.unpin(path: root)
            try require(unpinned?.ok == true && unpinned?.filesPinned == 1,
                        "production consumer unpin did not remove the temporary pin")
            print("PASS pin-unpin: temporary file registered and removed")
        default:
            throw ProbeError.failed("unknown mode")
        }
    }
}
