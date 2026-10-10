import Foundation

// Standalone source probe. Run only in a fresh AppIsolation root; this test
// intentionally exercises real preference persistence and production config.
@main
struct PreviewConfigProbe {
    static func main() throws {
        guard AppIsolation.active != nil else {
            fatalError("PreviewConfigProbe requires a fresh isolated app root")
        }
        let preferences = Preferences.load()
        precondition(!preferences.nativePreviewWarm, "missing preference must default off")
        precondition(preferences.replacementRecovery, "missing recovery override must default on")

        let initial = preferences.toServerConfig()
        let encoded = try JSONEncoder().encode(initial)
        var json = try JSONSerialization.jsonObject(with: encoded) as! [String: Any]
        precondition(json["native_preview_warm"] as? Bool == false,
                     "default-off config must explicitly encode false")
        json.removeValue(forKey: "native_preview_warm")
        let legacy = try JSONSerialization.data(withJSONObject: json)
        let decodedLegacy = try JSONDecoder().decode(NFSBridge.ServerConfig.self, from: legacy)
        precondition(!decodedLegacy.nativePreviewWarm, "old Swift JSON must decode as off")
        precondition(decodedLegacy == initial, "old Swift JSON changed another field")

        let initialObject = try JSONSerialization.jsonObject(with: encoded) as! [String: Any]
        precondition(initialObject["replacement_recovery"] as? Bool == true,
                     "recovery config must explicitly encode true")
        let recoveryValues: [Any?] = [nil, NSNull(), false, true]
        for value in recoveryValues {
            var object = initialObject
            object["replacement_recovery"] = value
            let data = try JSONSerialization.data(withJSONObject: object)
            let decoded = try JSONDecoder().decode(NFSBridge.ServerConfig.self, from: data)
            precondition(decoded.replacementRecovery == (value as? Bool ?? false),
                         "recovery config absent/null/Boolean decode changed")
        }
        var wrongType = initialObject
        wrongType["replacement_recovery"] = "true"
        let wrongTypeData = try JSONSerialization.data(withJSONObject: wrongType)
        precondition((try? JSONDecoder().decode(NFSBridge.ServerConfig.self, from: wrongTypeData)) == nil,
                     "recovery config accepted a non-Boolean")

        preferences.replacementRecovery = true
        let recoveryOn = Preferences.load()
        precondition(recoveryOn.replacementRecovery, "enabled recovery preference did not persist")
        let recoveryConfig = recoveryOn.toServerConfig()
        let recoveryJSON = try JSONEncoder().encode(recoveryConfig)
        let recoveryRoundTrip = try JSONDecoder().decode(NFSBridge.ServerConfig.self, from: recoveryJSON)
        precondition(recoveryRoundTrip == recoveryConfig,
                     "enabled recovery config did not round trip")
        recoveryOn.spoolEnabled = false
        precondition(!recoveryOn.toServerConfig().replacementRecovery,
                     "spool-off config emitted recovery enabled")
        recoveryOn.spoolEnabled = true
        recoveryOn.replacementRecovery = false
        precondition(!Preferences.load().toServerConfig().replacementRecovery,
                     "disabled recovery preference did not persist")

        preferences.nativePreviewWarm = true
        let reloadedOn = Preferences.load()
        precondition(reloadedOn.nativePreviewWarm, "enabled preference did not persist")
        let on = reloadedOn.toServerConfig()
        let onJSON = try JSONEncoder().encode(on)
        let decodedOn = try JSONDecoder().decode(NFSBridge.ServerConfig.self, from: onJSON)
        precondition(decodedOn == on,
                     "enabled config did not round trip")
        let onObject = try JSONSerialization.jsonObject(with: onJSON) as! [String: Any]
        precondition(onObject["native_preview_warm"] as? Bool == true,
                     "enabled config did not encode true")

        reloadedOn.nativePreviewWarm = false
        let reloadedOff = Preferences.load()
        precondition(!reloadedOff.nativePreviewWarm, "disabled preference did not persist")
        let off = reloadedOff.toServerConfig()
        let offJSON = try JSONEncoder().encode(off)
        let decodedOff = try JSONDecoder().decode(NFSBridge.ServerConfig.self, from: offJSON)
        precondition(decodedOff == off,
                     "disabled config did not round trip")
        let offObject = try JSONSerialization.jsonObject(with: offJSON) as! [String: Any]
        precondition(offObject["native_preview_warm"] as? Bool == false,
                     "disabled config did not encode false")
        print("PreviewConfigProbe PASS")
    }
}
