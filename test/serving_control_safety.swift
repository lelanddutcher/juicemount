import Foundation

@main
struct ServingControlSafetyFixture {
    static func main() throws {
        var deletes: [String] = []
        do {
            try ServingControlSafety.deleteMetadataFiles(at: "/fixture/metadata.db", serviceManaged: true) { deletes.append($0) }
            fatalError("service-owned metadata deletion admitted")
        } catch {
            precondition(deletes.isEmpty, "unknown service state unlinked files")
        }
        for bad in ["error: reply lost after request", "{}", "{\"ok\":true}", "{\"ok\":false,\"offline_mode\":true}", "{\"ok\":true,\"offline_mode\":null}"] {
            do { _ = try ServingControlSafety.offlineAcknowledgement(bad); fatalError("unacknowledged offline change admitted") }
            catch {}
        }
        let on = try ServingControlSafety.offlineAcknowledgement("{\"ok\":true,\"offline_mode\":true}")
        let off = try ServingControlSafety.offlineAcknowledgement("{\"ok\":true,\"offline_mode\":false}")
        precondition(on && !off)
        print("PASS: service custody refuses all unlink calls; five bad acknowledgements refused; two confirmed states decoded")
    }
}
