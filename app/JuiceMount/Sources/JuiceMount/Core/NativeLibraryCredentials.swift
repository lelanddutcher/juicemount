import Foundation
import Security

/// Delivery bearer custody is intentionally unrelated to Redis enrollment.
/// A missing item is distinct from an unreadable, migrated, or unstamped item.
struct NativeLibraryCredentials {
    enum Failure: Error { case deviceUnavailable, inaccessible, deviceMismatch, malformed, saveFailed }
    enum Lookup { case absent, present(Data) }
    private static let service = "com.juicemount.native-library"
    private static let stampPrefix = "jm-native-device:"
    var keychain: any KeychainOperations = SystemKeychain()
    var device: () -> String? = { DeviceFingerprint.current }

    private func stamp() throws -> String {
        guard let value = device(), NativeLibraryWire.lowerHexID(value, prefix: "", digits: 32) else {
            throw Failure.deviceUnavailable
        }
        return Self.stampPrefix + value
    }
    private func query(_ id: String) throws -> [String: Any] {
        guard NativeLibraryWire.lowerHexID(id, prefix: "", digits: 64) else { throw Failure.malformed }
        return [kSecClass as String: kSecClassGenericPassword,
                kSecAttrService as String: Self.service,
                kSecAttrAccount as String: id]
    }

    func read(profileID: String) throws -> Lookup {
        guard NativeLibraryWire.lowerHexID(profileID, prefix: "", digits: 64) else { throw Failure.malformed }
        let expected = try stamp()
        if let scope = AppIsolation.active {
            let path = scope.child("credential-native-library-" + profileID + ".json")
            let raw = try scope.readPrivateFile(path)
            guard let raw else { return .absent }
            struct Item: Codable { let profileID: String; let bearer: Data; let stamp: String }
            guard let item = try? JSONDecoder().decode(Item.self, from: raw), item.profileID == profileID,
                  item.stamp == expected, NativeLibraryWire.validBearer(item.bearer) else {
                throw Failure.deviceMismatch
            }
            return .present(item.bearer)
        }
        var q = try query(profileID)
        q[kSecReturnAttributes as String] = true
        q[kSecReturnData as String] = true
        q[kSecMatchLimit as String] = kSecMatchLimitOne
        q[kSecUseAuthenticationUI as String] = kSecUseAuthenticationUIFail
        let (status, found) = keychain.copyMatching(q)
        if status == errSecItemNotFound { return .absent }
        guard status == errSecSuccess, let attributes = found as? [String: Any],
              attributes[kSecAttrComment as String] as? String == expected,
              let bytes = attributes[kSecValueData as String] as? Data,
              NativeLibraryWire.validBearer(bytes) else {
            if status == errSecSuccess { throw Failure.deviceMismatch }
            throw Failure.inaccessible
        }
        return .present(bytes)
    }

    /// Save only after the exact profile tuple has been persisted. Existing
    /// items are updated only when their required device stamp already agrees.
    func save(_ bearer: Data, profileID: String) throws {
        guard NativeLibraryWire.lowerHexID(profileID, prefix: "", digits: 64),
              NativeLibraryWire.validBearer(bearer) else { throw Failure.malformed }
        let expected = try stamp()
        if let scope = AppIsolation.active {
            struct Item: Codable { let profileID: String; let bearer: Data; let stamp: String }
            let path = scope.child("credential-native-library-" + profileID + ".json")
            if let prior = try scope.readPrivateFile(path) {
                guard let item = try? JSONDecoder().decode(Item.self, from: prior),
                      item.profileID == profileID, item.stamp == expected else { throw Failure.deviceMismatch }
            }
            let bytes = try JSONEncoder().encode(Item(profileID: profileID, bearer: bearer, stamp: expected))
            try scope.writePrivateFile(bytes, to: path)
            return
        }
        let base = try query(profileID)
        var readQuery = base
        readQuery[kSecReturnAttributes as String] = true
        readQuery[kSecMatchLimit as String] = kSecMatchLimitOne
        readQuery[kSecUseAuthenticationUI as String] = kSecUseAuthenticationUIFail
        let (status, found) = keychain.copyMatching(readQuery)
        let values: [String: Any] = [kSecValueData as String: bearer,
                                      kSecAttrComment as String: expected]
        if status == errSecSuccess {
            guard let attributes = found as? [String: Any],
                  attributes[kSecAttrComment as String] as? String == expected,
                  keychain.update(base, values) == errSecSuccess else { throw Failure.deviceMismatch }
            return
        }
        guard status == errSecItemNotFound else { throw Failure.inaccessible }
        var me: SecTrustedApplication?
        guard SecTrustedApplicationCreateFromPath(nil, &me) == errSecSuccess, let me else {
            throw Failure.saveFailed
        }
        var access: SecAccess?
        guard SecAccessCreate("JuiceMount Native Library" as CFString, [me] as CFArray, &access) == errSecSuccess,
              let access else { throw Failure.saveFailed }
        var add = base.merging(values) { _, new in new }
        add[kSecAttrLabel as String] = "JuiceMount Native Library"
        add[kSecAttrAccess as String] = access
        guard keychain.add(add) == errSecSuccess else { throw Failure.saveFailed }
    }

    /// Credential removal is explicit and never deletes a pending copy,
    /// receipt, local file, or journal reservation.
    func remove(profileID: String) throws {
        guard NativeLibraryWire.lowerHexID(profileID, prefix: "", digits: 64) else { throw Failure.malformed }
        _ = try stamp()
        if let scope = AppIsolation.active {
            try scope.removePrivateFile(scope.child("credential-native-library-" + profileID + ".json"))
            return
        }
        let status = keychain.delete(try query(profileID))
        guard status == errSecSuccess || status == errSecItemNotFound else { throw Failure.inaccessible }
    }
}
