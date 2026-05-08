// Quick CLI test that calls the cbridge directly with a known config.
// Verifies whether the FUSE path makes it through Swift -> JSON -> Go.

import Foundation

// Mirror the Swift bridge struct
struct ServerConfig: Codable {
    var redisURL: String
    var fusePath: String
    var mountPoint: String
    var listenAddr: String
    var dbPath: String
    var cacheSize: String
    var metricsAddr: String

    enum CodingKeys: String, CodingKey {
        case redisURL = "redis_url"
        case fusePath = "fuse_path"
        case mountPoint = "mount_point"
        case listenAddr = "listen_addr"
        case dbPath = "db_path"
        case cacheSize = "cache_size"
        case metricsAddr = "metrics_addr"
    }
}

let cfg = ServerConfig(
    redisURL: "redis://192.168.0.210:6379/1",
    fusePath: "\(NSHomeDirectory())/.juicemount/fuse-internal",
    mountPoint: "/Volumes/zpool",
    listenAddr: "127.0.0.1:11049",
    dbPath: "\(NSHomeDirectory())/Library/Application Support/JuiceMount/metadata.db",
    cacheSize: "100000",
    metricsAddr: "127.0.0.1:11050"
)

let encoder = JSONEncoder()
encoder.outputFormatting = .prettyPrinted
let data = try encoder.encode(cfg)
let json = String(data: data, encoding: .utf8) ?? "<failed>"
print(json)
