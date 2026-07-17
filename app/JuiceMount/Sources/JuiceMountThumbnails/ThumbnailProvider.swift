//
//  ThumbnailProvider.swift
//  JuiceMountThumbnails — QuickLook thumbnail appex.
//
//  When Finder/QuickLook wants a thumbnail for a file on the JuiceMount
//  mount, macOS routes the request here instead of Apple's generator (which
//  would decode megabytes of remote video over NFS). We fetch the ~30KB
//  farm-pre-rendered poster that the JuiceMount daemon caches locally and
//  serves over loopback:
//
//      GET http://127.0.0.1:11050/thumb-local?path=<abs path>&size=<max px>
//        200 image/jpeg   -> draw it
//        404              -> not cached; the daemon kicks its own background
//                            warm — we do NOT retry or poll
//        connection error -> app not running
//
//  Contract: this appex is a CACHE IN FRONT of Apple's generator, never a
//  replacement that degrades correctness. On ANY miss/error/timeout it fails
//  fast with an error so macOS falls back to the built-in generator. It
//  never blocks past its tight timeouts, never retries, and never touches
//  the filesystem (no mount probes — the daemon answers 404 for paths it
//  doesn't own, which is the cheap way to scope ourselves to JuiceMount
//  volumes from inside the sandbox).
//

import AppKit
import Foundation
import ImageIO
import QuickLookThumbnailing
import os.log

final class ThumbnailProvider: QLThumbnailProvider {

    private static let daemonBase = "http://127.0.0.1:11050/thumb-local"

    /// One log line per request: outcome + basename + ms. The instant-nav
    /// GO/NO-GO measurement reads these via `log stream`; keep payloads small
    /// and never log the full path (basename only, %{public} so the harness
    /// can read it without private-data profiles).
    private static let log = OSLog(
        subsystem: "com.lelanddutcher.juicemount.thumbnails",
        category: "provide"
    )

    /// Shared session: tight timeouts, zero client-side caching (the daemon
    /// IS the cache), no connectivity waiting — a dead daemon must become a
    /// fast error, not a hang. The 1.5s request timeout is load-bearing: on a
    /// cache miss the daemon attempts a populated read-through before
    /// answering, so a slow miss is bounded here, not server-side.
    private static let session: URLSession = {
        let cfg = URLSessionConfiguration.ephemeral
        cfg.timeoutIntervalForRequest = 1.5
        cfg.timeoutIntervalForResource = 2.0
        cfg.waitsForConnectivity = false
        cfg.requestCachePolicy = .reloadIgnoringLocalAndRemoteCacheData
        cfg.urlCache = nil
        cfg.httpCookieStorage = nil
        return URLSession(configuration: cfg)
    }()

    /// Strict query-value encoder. `.urlQueryAllowed` leaves `+` and `&`
    /// intact, and Go's url.ParseQuery decodes `+` as a space — media
    /// basenames contain `+` often enough to matter. Percent-encode
    /// everything except unreserved characters (plus `/`, legal in query
    /// values and kept for daemon-side log readability).
    private static let queryValueAllowed: CharacterSet = {
        var cs = CharacterSet.alphanumerics
        cs.insert(charactersIn: "-._~/")
        return cs
    }()

    override func provideThumbnail(
        for request: QLFileThumbnailRequest,
        _ handler: @escaping (QLThumbnailReply?, Error?) -> Void
    ) {
        let started = DispatchTime.now()
        let basename = request.fileURL.lastPathComponent

        func elapsedMS() -> Double {
            Double(DispatchTime.now().uptimeNanoseconds &- started.uptimeNanoseconds) / 1_000_000
        }
        /// Single failure exit: one log line, then hand macOS an error so it
        /// falls back to Apple's built-in generator. Handler is called
        /// exactly once per request.
        func fail(_ outcome: String, code: Int) {
            os_log("%{public}@ %{public}@ %.1fms",
                   log: Self.log, type: .default, outcome, basename, elapsedMS())
            handler(nil, NSError(domain: "JuiceMountThumbnails", code: code,
                                 userInfo: [NSLocalizedDescriptionKey: outcome]))
        }

        let path = request.fileURL.path
        guard request.fileURL.isFileURL, path.hasPrefix("/") else {
            fail("error(not-a-file-url)", code: 400)
            return
        }

        // Requested pixel budget: maximumSize is in POINTS; scale converts to
        // device pixels. The daemon clamps to what the farm rendered.
        let pixelSize = Int((max(request.maximumSize.width,
                                 request.maximumSize.height) * request.scale).rounded(.up))

        guard
            let encodedPath = path.addingPercentEncoding(withAllowedCharacters: Self.queryValueAllowed),
            let url = URL(string: "\(Self.daemonBase)?path=\(encodedPath)&size=\(pixelSize)")
        else {
            fail("error(bad-url)", code: 400)
            return
        }

        var req = URLRequest(url: url)
        req.httpMethod = "GET"
        req.timeoutInterval = 1.5

        // Synchronous fetch via semaphore: provideThumbnail is already off
        // the main thread on a QuickLook worker queue, and the session
        // timeouts strictly bound the wait.
        var body: Data?
        var status = 0
        var fetchError: Error?
        let sem = DispatchSemaphore(value: 0)
        let task = Self.session.dataTask(with: req) { data, resp, err in
            body = data
            status = (resp as? HTTPURLResponse)?.statusCode ?? 0
            fetchError = err
            sem.signal()
        }
        task.resume()
        // Belt over the session's braces: never park QuickLook's worker even
        // if URLSession misbehaves. 2.5s > timeoutIntervalForResource, so in
        // practice the session always signals first.
        if sem.wait(timeout: .now() + 2.5) == .timedOut {
            task.cancel()
            fail("error(timeout)", code: 408)
            return
        }

        if let err = fetchError {
            switch (err as NSError).code {
            case NSURLErrorCannotConnectToHost, NSURLErrorNetworkConnectionLost:
                fail("error(daemon-down)", code: 503) // app not running
            case NSURLErrorTimedOut:
                fail("error(timeout)", code: 408)
            default:
                fail("error(fetch)", code: 502)
            }
            return
        }
        if status == 404 {
            // Daemon doesn't have it (or doesn't own the path). It kicks its
            // own background warm; Apple's generator covers this request.
            fail("miss", code: 404)
            return
        }
        guard status == 200, let data = body, !data.isEmpty else {
            fail("error(http-\(status))", code: 502)
            return
        }
        guard
            let src = CGImageSourceCreateWithData(
                data as CFData, [kCGImageSourceShouldCache: false] as CFDictionary),
            let image = CGImageSourceCreateImageAtIndex(
                src, 0, [kCGImageSourceShouldCacheImmediately: true] as CFDictionary),
            image.width > 0, image.height > 0
        else {
            fail("error(decode)", code: 500)
            return
        }

        // Reply sizing. contextSize is in POINTS and QuickLook requires it to
        // fit within request.maximumSize. Aspect-fit the poster's aspect
        // ratio into maximumSize so one dimension fills the budget (upscaling
        // allowed: Finder expects the size it asked for), then clamp into
        // [minimumSize, maximumSize] — the min-clamp only bites on
        // pathological aspect ratios and merely letterboxes. The drawing
        // context QuickLook hands us is already scaled by request.scale, so
        // drawing the full-res pixels into this point-sized rect yields a
        // sharp Retina thumbnail without touching scale ourselves.
        let maxSize = request.maximumSize
        let minSize = request.minimumSize
        let iw = CGFloat(image.width)
        let ih = CGFloat(image.height)
        let fit = min(maxSize.width / iw, maxSize.height / ih)
        var cw = (iw * fit).rounded(.down)
        var ch = (ih * fit).rounded(.down)
        cw = min(max(cw, min(minSize.width, maxSize.width)), maxSize.width)
        ch = min(max(ch, min(minSize.height, maxSize.height)), maxSize.height)
        let contextSize = CGSize(width: max(cw, 1), height: max(ch, 1))

        let reply = QLThumbnailReply(contextSize: contextSize) { () -> Bool in
            // Runs later on QuickLook's drawing pass with the thumbnail
            // context current. Draw aspect-fit centered: normally exact
            // (contextSize derives from the image aspect), letterboxed only
            // when the minimumSize clamp forced the context off-aspect.
            guard let ctx = NSGraphicsContext.current?.cgContext else { return false }
            let scale = min(contextSize.width / iw, contextSize.height / ih)
            let dw = iw * scale
            let dh = ih * scale
            let rect = CGRect(x: (contextSize.width - dw) / 2,
                              y: (contextSize.height - dh) / 2,
                              width: dw, height: dh)
            ctx.interpolationQuality = .high
            ctx.draw(image, in: rect)
            return true
        }
        os_log("hit %{public}@ %.1fms %dpx",
               log: Self.log, type: .default, basename, elapsedMS(), pixelSize)
        handler(reply, nil)
    }
}
