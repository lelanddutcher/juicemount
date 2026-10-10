import Foundation

// Standalone source probe for H-8B Mac enrollment (the app side of
// bridge/enrollment.go). Built like RedisCredentialMigrationProbe.swift:
//
//   swiftc -parse-as-library -I <dir with a JuiceMountCore module.modulemap> \
//     -Xlinker -undefined -Xlinker dynamic_lookup -o <scratch>/enroll-probe \
//     Tests/EnrollmentCredentialProbe.swift Sources/JuiceMount/Core/*.swift (minus ServerController.swift)
//
// and run only in a fresh AppIsolation root (env as for that probe). It never
// calls into libnfsd and never contacts a Manager.
//
// Checks: the object-key storage form matches Go's credstore encoding; the
// enrollment record has the Go client's JSON shape; an approval the server
// does not accept yet (credentials_live false: before the proxy cutover)
// changes NOTHING, on every poll, and says "waiting for the server cutover";
// an approval the server accepts first copies the working login to its own
// slot, then switches the Redis URL to the principal and stores the enrollment
// secret as the Redis password and the object key, all bound to the
// principal's account, and keeps every secret out of the preferences plist;
// a switched Mac whose login the server refuses shows the reason, not a green
// "Approved"; a login changed by hand (admin) is never switched back by a
// poll and never copied to the previous-login slot (only the first switch
// copies); "Use previous login" restores the pre-cutover login, clears that
// slot and pauses switching; a revoke clears the slot too; a
// failed copy of the working login switches nothing; the engine config
// carries the object key only for that account; a fresh load reads the object
// key back; a revoke stops handing it out; the Manager address is derived from
// the Redis host when none is set; Settings' status lines name the principal
// and the code the admin must match. Per-Mac storage keys (H-8): a
// mac-<12 hex>-<gen> key with a MinIO-generated '+'/'/' secret is stored
// exactly and replaces the old one, applying at restart ("new storage key
// will be used after restart"); a pending key keeps the stored one; a
// refusal with poll_now runs an enrollment step at once, without poll_now it
// does not; the refusal note is truthful about the spool (on: new files
// stay on this Mac, changes to existing files fail, a new file saved over an
// existing one can look empty elsewhere; off: writes fail); a known status
// without a refusal (the bridge expired it) clears the red state; revoked +
// refused shows both.
//
// The fake store models the real one: ONE item per slot (RedisCredentialStore
// keeps one Keychain item per slot, bound to one account), so overwriting a
// slot for a new account destroys the old secret, as it does in the Keychain.

// Core/PermissionProbes.swift references this UI type; the probe never calls it.
enum OnboardingPreflight {
    enum ParseResult {
        case success((String, UInt16))
        case failure(String)
    }
    static func parseRedisHostPort(_ raw: String) -> ParseResult { .failure("probe stub") }
}

/// One item per slot, like the Keychain store: a write for another account
/// replaces the only item.
final class SlotStore: RedisCredentialStoring {
    var item: (account: String, secret: String)?
    var refuseWrites = false
    func password(for account: String) -> String? {
        guard let item, item.account == account, !item.secret.isEmpty else { return nil }
        return item.secret
    }
    func setPassword(_ password: String, for account: String) -> Bool {
        if refuseWrites { return false }
        item = (account, password)
        return true
    }
}

func passwordSaved(_ p: Preferences) -> Bool {
    if case .saved = p.redisPasswordStatus { return true }
    return false
}

@main
struct EnrollmentCredentialProbe {
    static func check(_ ok: Bool, _ message: String) {
        if !ok {
            FileHandle.standardError.write(Data(("FAIL: " + message + "\n").utf8))
            exit(1)
        }
    }

    static func main() throws {
        guard AppIsolation.active != nil else {
            fatalError("EnrollmentCredentialProbe requires a fresh isolated app root")
        }
        DeviceFingerprint.platformUUID = { "AAAAAAAA-0000-4000-8000-00000000000A" }

        // Object-key storage form (credstore.EncodeObjectKey/DecodeObjectKey).
        check(ObjectKeyCredential.encode(accessKey: "jm-client", secretKey: "s3cr/et+key=") == "jm-client\ns3cr/et+key=",
              "object key encoding differs from Go's")
        let decoded = ObjectKeyCredential.decode("jm-client\ns3cr/et+key=")
        check(decoded?.accessKey == "jm-client" && decoded?.secretKey == "s3cr/et+key=", "object key decode")
        for bad in ["", "ak", "\nsk", "ak\n", "ak\nsk\nmore", "ak\r\nsk"] {
            check(ObjectKeyCredential.decode(bad) == nil, "decode accepted \(bad.debugDescription)")
        }
        check(ObjectKeyCredential.encode(accessKey: "a", secretKey: "s\r\nk") == nil, "encode accepted a CRLF secret")

        // Enrollment record: the Go client's State JSON (internal/enrollclient).
        let goState = #"{"manager_url":"http://nas.local:30190","manager_id":"0123456789abcdef0123456789abcdef","instance_id":"4F1C2A9E-7B3D-4C1A-9E2B-0A1B2C3D4E5F","secret":"sec","poll_token":"tok","principal":"mac-4f1c2a9e7b3d","fingerprint":"ABCD-1234","status":"pending","failures":2}"#
        guard let record = EnrollmentRecord.load(goState) else { check(false, "Go state did not decode"); return }
        check(record.secret == "sec" && record.pollToken == "tok" && record.principal == "mac-4f1c2a9e7b3d" && record.failures == 2,
              "record fields: \(record.principal)")
        // The Manager ID binding round-trips (review 3 finding 6): dropping it
        // would unbind the record on every step.
        check(record.managerID == "0123456789abcdef0123456789abcdef", "manager_id dropped: \(record.managerID)")
        let reencoded = try JSONSerialization.jsonObject(with: Data(record.stored!.utf8)) as? [String: Any]
        check(Set(reencoded?.keys.map { $0 } ?? []) == ["manager_url", "manager_id", "instance_id", "secret", "poll_token", "principal", "fingerprint", "status", "failures"],
              "record keys differ from the Go client's: \(String(describing: reencoded?.keys))")

        // Approval.
        let passwords = SlotStore(), objectKeys = SlotStore(), link = SlotStore(), enroll = SlotStore(), previous = SlotStore()
        Preferences.credentialStore = passwords
        Preferences.objectKeyStore = objectKeys
        Preferences.linkCredentialStore = link
        Preferences.enrollmentStore = enroll
        Preferences.previousLoginStore = previous
        // The pre-cutover login: a shared user with its password in the
        // metadata slot (the only copy).
        let legacyURL = "redis://jmclient@nas.local:30179/1", legacyPassword = "pre-cutover-shared-pw"
        let legacyAccount = "redis://jmclient@nas.local:30179"
        _ = passwords.setPassword(legacyPassword, for: legacyAccount)
        AppIsolation.preferences.set(legacyURL, forKey: "redisURL")
        let prefs = Preferences.load()
        check(passwordSaved(prefs), "legacy password not loaded")
        check(prefs.effectiveManagerURL == "http://nas.local:30190", "derived Manager URL = \(prefs.effectiveManagerURL ?? "nil")")
        prefs.managerURL = "https://manager.example"
        check(prefs.effectiveManagerURL == "https://manager.example", "typed Manager URL ignored")
        prefs.managerURL = ""
        check(prefs.toServerConfig().objectAccessKey.isEmpty, "object key before enrollment")

        let principal = "mac-4f1c2a9e7b3d", secret = "Zm9vYmFyYmF6cXV4cXV1eHF1dXhxdXV4cXV1eHF1dXg"
        let account = "redis://\(principal)@nas.local:30179"
        func approvedStep(live: Bool?) throws -> EnrollStepOutput {
            let liveField = live.map { #","credentials_live":\#($0)"# } ?? ""
            let check = live == true ? "" : #","credential_check":"the server does not accept this Mac's new login yet (it has not switched to the per-Mac proxy)""#
            return try JSONDecoder().decode(EnrollStepOutput.self, from: Data(#"""
                {"state":{"manager_url":"http://nas.local:30190","instance_id":"4F1C2A9E-7B3D-4C1A-9E2B-0A1B2C3D4E5F","secret":"\#(secret)","poll_token":"tok","principal":"\#(principal)","fingerprint":"ABCD-1234","status":"approved"},
                 "persist":true,"phase":"approved","object_access_key":"jm-client","object_secret_key":"objsecret","retry_after_ms":1800000\#(liveField)\#(check)}
                """#.utf8))
        }

        // Approved before the cutover (the server refuses the principal):
        // nothing changes, on the first poll or any later one.
        for (i, live) in [false, nil, false].enumerated() {
            let early = try approvedStep(live: live)
            prefs.applyEnrollmentStep(early, record: early.state)
            Preferences.credentialWrites.sync {}
            check(prefs.redisURL == legacyURL, "poll \(i): pre-cutover approval switched the URL to \(prefs.redisURL)")
            check(passwords.password(for: legacyAccount) == legacyPassword, "poll \(i): pre-cutover approval destroyed the working password")
            check(passwordSaved(prefs), "poll \(i): working password no longer in use")
            check(objectKeys.item == nil && !prefs.hasObjectKey, "poll \(i): object key stored before the cutover")
            check(prefs.enrollment.phase == .approvedPendingCutover, "poll \(i): phase \(prefs.enrollment.phase)")
            check(prefs.enrollment.summary.contains("waiting for the server cutover"), "poll \(i): status = \(prefs.enrollment.summary)")
        }

        // A failed copy of the working login switches nothing.
        previous.refuseWrites = true
        let liveOut = try approvedStep(live: true)
        prefs.applyEnrollmentStep(liveOut, record: liveOut.state)
        Preferences.credentialWrites.sync {}
        check(prefs.redisURL == legacyURL && passwords.password(for: legacyAccount) == legacyPassword,
              "switched although the working login could not be copied")
        check(prefs.enrollment.phase == .failed, "phase after a refused copy = \(prefs.enrollment.phase)")
        previous.refuseWrites = false

        // After the cutover the server accepts the principal: switch, with the
        // working login kept for a rollback.
        let out = liveOut
        prefs.applyEnrollmentStep(out, record: out.state)
        Preferences.credentialWrites.sync {}
        check(prefs.enrollment.phase == .approved && prefs.enrollment.summary.contains(principal), "status = \(prefs.enrollment.summary)")
        let kept = PreviousLogin.load(previous.password(for: PreviousLogin.account))
        check(kept == PreviousLogin(redisURL: legacyURL, password: legacyPassword), "pre-cutover login not kept: \(String(describing: kept?.redisURL))")
        check(prefs.canRestorePreviousLogin, "Settings does not offer the previous login")
        check(prefs.redisURL == "redis://\(principal)@nas.local:30179/1", "redisURL = \(prefs.redisURL)")
        check(passwords.password(for: account) == secret, "the enrollment secret was not stored as the Redis password for \(account)")
        check(objectKeys.password(for: account) == "jm-client\nobjsecret", "object key not stored for \(account)")
        let saved = AppIsolation.preferences.string(forKey: "redisURL") ?? ""
        check(saved == "redis://\(principal)@nas.local:30179/1", "plist redisURL = \(saved)")
        let plist = AppIsolation.active!.root.appendingPathComponent("preferences.plist")
        let values = (try PropertyListSerialization.propertyList(from: Data(contentsOf: plist), format: nil) as? [String: Any]) ?? [:]
        check(!values.isEmpty, "isolated preferences plist not found; the secret scan would prove nothing")
        for (k, v) in values {
            let text = "\(v)"
            check(!text.contains(secret) && !text.contains("objsecret") && !text.contains(legacyPassword), "preferences key \(k) holds a secret")
        }
        let cfg = prefs.toServerConfig()
        check(cfg.objectAccessKey == "jm-client" && cfg.objectSecretKey == "objsecret", "engine config lacks the object key")
        check(RedisURLCredential.split(cfg.redisURL).password == secret, "engine config lacks the Redis password")

        // A config for another endpoint never carries the key.
        prefs.redisURL = "redis://\(principal)@other.local:30179/1"
        check(prefs.toServerConfig().objectSecretKey.isEmpty, "object key attached to another endpoint")
        prefs.redisURL = "redis://\(principal)@nas.local:30179/1"

        // F5a: a switched Mac whose login the server refuses is not shown as
        // a green "Approved"; the reason is shown and nothing is switched.
        let refusedOut = try approvedStep(live: false)
        prefs.applyEnrollmentStep(refusedOut, record: refusedOut.state)
        Preferences.credentialWrites.sync {}
        check(prefs.enrollment.phase == .approvedLoginRefused, "switched Mac refused by the server shows phase \(prefs.enrollment.phase)")
        check(prefs.enrollment.summary.contains("did not accept this Mac's login") &&
              prefs.enrollment.summary.contains("does not accept this Mac's new login yet"),
              "refused login status hides the reason: \(prefs.enrollment.summary)")
        check(prefs.redisURL == "redis://\(principal)@nas.local:30179/1" && passwords.password(for: account) == secret,
              "a refused check changed the login")
        prefs.applyEnrollmentStep(out, record: out.state)
        check(prefs.enrollment.phase == .approved, "accepted again: phase \(prefs.enrollment.phase)")

        // Review 3 finding 5: a hand edit to a URL with NO user name (Redis's
        // "default" user, the onboarding form's login) is a hand edit too.
        // Approved polls neither switch it back nor overwrite its password.
        let defaultURL = "redis://nas.local:30179/1", defaultPassword = "hand-typed-default-user-pw"
        prefs.redisURL = defaultURL
        prefs.setRedisPassword(defaultPassword)
        Preferences.credentialWrites.sync {}
        for i in 0..<2 {
            prefs.applyEnrollmentStep(out, record: out.state)
            Preferences.credentialWrites.sync {}
            check(prefs.redisURL == defaultURL, "poll \(i): a user-less hand edit was switched back to \(prefs.redisURL)")
            check(passwords.item?.secret == defaultPassword, "poll \(i): the password typed for the user-less login was overwritten")
            check(prefs.enrollment.phase == .approvedNotInUse && prefs.enrollment.summary.contains("\"default\""),
                  "poll \(i): user-less hand edit status = \(prefs.enrollment.phase) \(prefs.enrollment.summary)")
        }

        // F5b: the user changes the login by hand (here to admin). Approved
        // polls do not switch it back, and the admin password never reaches
        // the previous-login slot, not even when Request access switches.
        let adminURL = "redis://admin@nas.local:30179/1", adminPassword = "hand-entered-admin-pw"
        prefs.redisURL = adminURL
        prefs.setRedisPassword(adminPassword)
        Preferences.credentialWrites.sync {}
        for i in 0..<2 {
            prefs.applyEnrollmentStep(out, record: out.state)
            Preferences.credentialWrites.sync {}
            check(prefs.redisURL == adminURL, "poll \(i): a hand-edited login was switched back to \(prefs.redisURL)")
            check(prefs.enrollment.phase == .approvedNotInUse && prefs.enrollment.summary.contains("\"admin\""),
                  "poll \(i): hand edit status = \(prefs.enrollment.phase) \(prefs.enrollment.summary)")
        }
        prefs.explicitSwitchRequested = true // what Request access does
        prefs.applyEnrollmentStep(out, record: out.state)
        Preferences.credentialWrites.sync {}
        check(prefs.redisURL == "redis://\(principal)@nas.local:30179/1" && passwords.password(for: account) == secret,
              "Request access after a hand edit did not switch")
        let keptAfterEdit = PreviousLogin.load(previous.password(for: PreviousLogin.account))
        check(keptAfterEdit == PreviousLogin(redisURL: legacyURL, password: legacyPassword),
              "the previous-login slot no longer holds the pre-enrollment login: \(String(describing: keptAfterEdit?.redisURL))")
        check(!(previous.item?.secret ?? "").contains(adminPassword), "the hand-entered admin password was copied to the previous-login slot")

        // "Use previous login": the pre-cutover login comes back, the slot is
        // cleared (F5c), and the next approved poll does not switch again
        // until Request access.
        check(prefs.restorePreviousLogin(), "restore refused")
        Preferences.credentialWrites.sync {}
        check(prefs.redisURL == legacyURL && passwords.password(for: legacyAccount) == legacyPassword, "restore did not bring the old login back")
        check(prefs.toServerConfig().objectSecretKey.isEmpty, "restored login hands out the principal's object key")
        check(prefs.previousLogin == nil && !prefs.canRestorePreviousLogin, "the previous-login slot outlived a successful restore")
        prefs.applyEnrollmentStep(out, record: out.state)
        Preferences.credentialWrites.sync {}
        check(prefs.redisURL == legacyURL && prefs.enrollment.phase == .approvedNotInUse, "a paused Mac switched again: \(prefs.redisURL)")
        prefs.enrollmentAdoptionPaused = false // what Request access does
        prefs.explicitSwitchRequested = true
        prefs.applyEnrollmentStep(out, record: out.state)
        Preferences.credentialWrites.sync {}
        check(prefs.redisURL == "redis://\(principal)@nas.local:30179/1" && passwords.password(for: account) == secret, "Request access did not switch back")
        check(prefs.previousLogin == nil, "a later switch copied a login into the previous-login slot")

        // A fresh load (next launch) reads the key back for the account.
        _ = previous.setPassword(PreviousLogin(redisURL: legacyURL, password: legacyPassword).stored!, for: PreviousLogin.account)
        let again = Preferences.load()
        check(again.canRestorePreviousLogin, "a kept previous login is not offered after launch")
        check(again.toServerConfig().objectSecretKey == "objsecret", "object key not reloaded at launch")

        // Revoked: the key is no longer handed to juicefs, and is cleared.
        let revoked = try JSONDecoder().decode(EnrollStepOutput.self, from: Data(#"""
            {"state":{"principal":"\#(principal)","status":"revoked","secret":"\#(secret)"},"persist":true,"phase":"revoked","retry_after_ms":600000}
            """#.utf8))
        again.applyEnrollmentStep(revoked, record: revoked.state)
        Preferences.credentialWrites.sync {}
        check(again.enrollment.phase == .revoked && again.toServerConfig().objectSecretKey.isEmpty, "revoked Mac still hands out the object key")
        check(ObjectKeyCredential.decode(objectKeys.password(for: account) ?? "") == nil, "revoked object key still stored")
        check(again.previousLogin == nil && !again.canRestorePreviousLogin, "a revoke kept the previous-login password")
        // Review 3 finding 8: the revoke also ends the switch. After it the
        // user logs in by hand again; a new approval is a first switch: it
        // keeps that login and offers "Use previous login".
        check(again.enrollmentSwitchedPrincipal.isEmpty, "a revoke kept the switched-principal marker: \(again.enrollmentSwitchedPrincipal)")
        let reURL = "redis://jmclient@nas.local:30179/1", rePassword = "re-entered-shared-pw"
        again.redisURL = reURL
        again.setRedisPassword(rePassword)
        Preferences.credentialWrites.sync {}
        again.applyEnrollmentStep(out, record: out.state)
        Preferences.credentialWrites.sync {}
        check(again.enrollment.phase == .approved && again.redisURL == "redis://\(principal)@nas.local:30179/1",
              "re-approval after a revoke did not switch: \(again.enrollment.phase) \(again.redisURL)")
        check(PreviousLogin.load(previous.password(for: PreviousLogin.account)) == PreviousLogin(redisURL: reURL, password: rePassword) &&
              again.canRestorePreviousLogin, "re-approval after a revoke did not keep the working login")

        // H-8 per-Mac storage keys (design §5 "Mac side", §7 M3).
        let gen1 = "\(principal)-1", gen2 = "\(principal)-2"
        let plusSlashValue = "fixture+not/a+key/fixture+not/a+key/fixture" // MinIO-generated: '+' and '/'
        func perMacStep(_ ak: String?, inUse: String?, pending: Bool = false, refused: String? = nil) throws -> EnrollStepOutput {
            var fields = ""
            if let ak { fields += #","object_access_key":"\#(ak)","object_secret_key":"\#(plusSlashValue)""# }
            if let inUse { fields += #","object_key_in_use":"\#(inUse)""# }
            if pending { fields += #","object_key_pending":true"# }
            if let refused { fields += #","object_key_refused":true,"refused_code":"\#(refused)""# }
            return try JSONDecoder().decode(EnrollStepOutput.self, from: Data(#"""
                {"state":{"manager_url":"http://nas.local:30190","instance_id":"4F1C2A9E-7B3D-4C1A-9E2B-0A1B2C3D4E5F","secret":"\#(secret)","poll_token":"tok","principal":"\#(principal)","fingerprint":"ABCD-1234","status":"approved"},
                 "persist":false,"phase":"approved","retry_after_ms":1800000,"credentials_live":true\#(fields)}
                """#.utf8))
        }
        // Shared key in use and delivered: nothing to say (the shared path is unchanged).
        var step = try perMacStep(nil, inUse: "jm-client")
        again.applyEnrollmentStep(step, record: step.state)
        Preferences.credentialWrites.sync {}
        check(again.enrollment.storageKey == .normal && again.enrollment.storageKeyNote == nil,
              "shared key in use: \(again.enrollment.storageKey)")
        // This Mac's gen-1 key arrives while juicefs runs the shared key: it
        // replaces the stored key (secret with '+' intact) and applies at restart.
        step = try perMacStep(gen1, inUse: "jm-client")
        again.applyEnrollmentStep(step, record: step.state)
        Preferences.credentialWrites.sync {}
        check(objectKeys.password(for: account) == "\(gen1)\n\(plusSlashValue)", "per-Mac key not stored exactly: \(objectKeys.item?.secret.count ?? 0) chars")
        check(again.toServerConfig().objectAccessKey == gen1 && again.toServerConfig().objectSecretKey == plusSlashValue,
              "the next start does not hand juicefs the per-Mac key")
        check(again.enrollment.storageKey == .restartNeeded &&
              (again.enrollment.storageKeyNote ?? "").contains("new storage key will be used after restart"),
              "replaced key: \(again.enrollment.storageKey) \(again.enrollment.storageKeyNote ?? "nil")")
        // After the restart juicefs runs gen 1: nothing to say.
        step = try perMacStep(gen1, inUse: gen1)
        again.applyEnrollmentStep(step, record: step.state)
        check(again.enrollment.storageKey == .normal, "key in use: \(again.enrollment.storageKey)")
        // Pending (C1): no key delivered; the stored key stays; say so.
        step = try perMacStep(nil, inUse: gen1, pending: true)
        again.applyEnrollmentStep(step, record: step.state)
        Preferences.credentialWrites.sync {}
        check(again.toServerConfig().objectAccessKey == gen1 && objectKeys.password(for: account) == "\(gen1)\n\(plusSlashValue)",
              "a pending answer changed the stored key")
        check(again.enrollment.storageKey == .pending && (again.enrollment.storageKeyNote ?? "").contains("still creating"),
              "pending: \(again.enrollment.storageKey)")

        // Refusal: the status check reports a new refusal with poll_now; the
        // app runs an enrollment step at once (not in 30 minutes).
        let stepRan = DispatchSemaphore(value: 0)
        var calls: [String] = []
        let callsLock = NSLock()
        Preferences.enrollStepCall = { input in
            callsLock.lock(); calls.append(input); callsLock.unlock()
            if input.contains(#""op":"object-key-status""#) {
                return #"{"known":true,"object_key_in_use":"\#(gen1)","object_key_refused":true,"refused_code":"InvalidAccessKeyId","poll_now":true}"#
            }
            stepRan.signal()
            return nil // the probe does not run the bridge; the step's own result is not under test here
        }
        let statusDone = DispatchSemaphore(value: 0)
        again.applyObjectKeyStatus(try JSONDecoder().decode(ObjectKeyStatusOutput.self, from: Data(
            #"{"known":true,"object_key_in_use":"\#(gen1)","object_key_refused":true,"refused_code":"InvalidAccessKeyId","poll_now":true}"#.utf8)))
        statusDone.signal()
        check(stepRan.wait(timeout: .now() + 5) == .success, "a refusal with poll_now did not run an enrollment step")
        callsLock.lock()
        let stepInput = calls.last ?? ""
        callsLock.unlock()
        check(!stepInput.contains("object-key-status") && stepInput.contains("manager_url"), "the triggered call was not an enrollment step: \(stepInput.prefix(60))")
        check(again.enrollment.storageKey == .refused(code: "InvalidAccessKeyId", replaced: false), "refused: \(again.enrollment.storageKey)")
        let refusedNote = again.enrollment.storageKeyNote ?? ""
        check(refusedNote.contains("This Mac's storage access was refused") &&
              refusedNote.contains("new files stay on this Mac") &&
              refusedNote.contains("upload after access is restored and the volume restarts") &&
              refusedNote.contains("Changes to files that already exist fail while access is refused") &&
              refusedNote.contains("A new file saved over an existing one can look empty or incomplete on other Macs") &&
              !refusedNote.contains("new writes stay on this Mac") &&
              refusedNote.contains(principal), "refused note: \(refusedNote)")
        // Without poll_now (the bridge's rate limit) no step runs.
        calls.removeAll()
        again.applyObjectKeyStatus(try JSONDecoder().decode(ObjectKeyStatusOutput.self, from: Data(
            #"{"known":true,"object_key_in_use":"\#(gen1)","object_key_refused":true,"refused_code":"InvalidAccessKeyId"}"#.utf8)))
        check(stepRan.wait(timeout: .now() + 0.5) == .timedOut, "a refusal without poll_now ran a step")
        // An unknown status (no serving process) changes nothing.
        again.applyObjectKeyStatus(try JSONDecoder().decode(ObjectKeyStatusOutput.self, from: Data(#"{"known":false}"#.utf8)))
        check(again.enrollment.storageKey == .refused(code: "InvalidAccessKeyId", replaced: false), "unknown status cleared the refusal")
        // A known status without a refusal (the bridge expires one 5 minutes
        // after the last) clears the red state without a restart.
        again.applyObjectKeyStatus(try JSONDecoder().decode(ObjectKeyStatusOutput.self, from: Data(
            #"{"known":true,"object_key_in_use":"\#(gen1)"}"#.utf8)))
        var stillRefused = false
        if case .refused = again.enrollment.storageKey { stillRefused = true }
        check(!stillRefused && !(again.enrollment.storageKeyNote ?? "").contains("refused"),
              "an expired refusal kept the red state: \(again.enrollment.storageKey)")
        // The Manager answers with a new key (rotation): replaced, restart to use it.
        step = try perMacStep(gen2, inUse: gen1, refused: "InvalidAccessKeyId")
        again.applyEnrollmentStep(step, record: step.state)
        Preferences.credentialWrites.sync {}
        check(again.enrollment.storageKey == .refused(code: "InvalidAccessKeyId", replaced: true) &&
              (again.enrollment.storageKeyNote ?? "").contains("Restart the volume"), "replaced after refusal: \(again.enrollment.storageKeyNote ?? "nil")")
        // With the spool off, the note does not promise that writes are kept.
        again.spoolEnabled = false
        step = try perMacStep(gen1, inUse: gen1, refused: "AccessDenied")
        again.applyEnrollmentStep(step, record: step.state)
        check(!(again.enrollment.storageKeyNote ?? "").contains("stay on this Mac") &&
              (again.enrollment.storageKeyNote ?? "").contains("fail until access is restored"), "spool off: \(again.enrollment.storageKeyNote ?? "nil")")
        again.spoolEnabled = true
        // Revoked: the existing Revoked line plus the refusal; the key is forgotten.
        let revokedRefused = try JSONDecoder().decode(EnrollStepOutput.self, from: Data(#"""
            {"state":{"principal":"\#(principal)","status":"revoked","secret":"\#(secret)"},"persist":true,"phase":"revoked","retry_after_ms":600000,"object_key_in_use":"\#(gen1)","object_key_refused":true,"refused_code":"InvalidAccessKeyId"}
            """#.utf8))
        again.applyEnrollmentStep(revokedRefused, record: revokedRefused.state)
        Preferences.credentialWrites.sync {}
        check(again.enrollment.phase == .revoked && again.enrollment.summary.contains("Revoked") &&
              again.enrollment.storageKey == .refused(code: "InvalidAccessKeyId", replaced: false) &&
              (again.enrollment.storageKeyNote ?? "").contains("new files stay on this Mac"),
              "revoked + refused: \(again.enrollment.storageKey) \(again.enrollment.storageKeyNote ?? "nil")")
        check(again.toServerConfig().objectSecretKey.isEmpty, "a revoked Mac still hands out the per-Mac key")
        _ = statusDone

        // Waiting: the admin's code and the principal are shown.
        var waiting = EnrollmentStatus(phase: .waiting, principal: principal, fingerprint: "ABCD-1234")
        check(waiting.summary.contains("ABCD-1234") && waiting.summary.contains(principal), "waiting line = \(waiting.summary)")
        waiting.phase = .notEnrolled
        check(!waiting.summary.isEmpty, "empty status line")
        print("EnrollmentCredentialProbe PASS")
    }
}
