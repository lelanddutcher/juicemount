import AppKit
import Carbon.HIToolbox

/// Registers a global hotkey (default: ⌘⇧F) that opens the search window from
/// any app. Uses Carbon Event Manager because that's the only API for global
/// hotkeys on macOS without requiring Accessibility permissions.
@MainActor
final class HotkeyManager {

    static let shared = HotkeyManager()

    private var hotKeyRef: EventHotKeyRef?
    private var handler: EventHandlerRef?
    private var callback: (() -> Void)?

    private let signature: OSType = 0x4A4D434E // 'JMCN' — JuiceMount Cmd-shift-N
    private let id: UInt32 = 1

    private init() {}

    /// Register ⌘⇧F as the global hotkey. The closure is invoked on the main thread.
    func register(callback: @escaping () -> Void) {
        unregister()
        self.callback = callback

        let hotKeyID = EventHotKeyID(signature: signature, id: id)

        let modifiers: UInt32 = UInt32(cmdKey | shiftKey)
        let keyCode: UInt32 = UInt32(kVK_ANSI_F)

        var status = RegisterEventHotKey(
            keyCode,
            modifiers,
            hotKeyID,
            GetApplicationEventTarget(),
            0,
            &hotKeyRef
        )

        guard status == noErr else {
            NSLog("HotkeyManager: failed to register hotkey, status=\(status)")
            return
        }

        var spec = EventTypeSpec(
            eventClass: OSType(kEventClassKeyboard),
            eventKind: UInt32(kEventHotKeyPressed)
        )

        let userData = Unmanaged.passUnretained(self).toOpaque()
        status = InstallEventHandler(
            GetApplicationEventTarget(),
            { _, event, userData in
                guard let event, let userData else { return OSStatus(eventNotHandledErr) }
                var hotKeyID = EventHotKeyID()
                let result = GetEventParameter(
                    event,
                    EventParamName(kEventParamDirectObject),
                    EventParamType(typeEventHotKeyID),
                    nil,
                    MemoryLayout<EventHotKeyID>.size,
                    nil,
                    &hotKeyID
                )
                guard result == noErr else { return result }

                let me = Unmanaged<HotkeyManager>.fromOpaque(userData).takeUnretainedValue()
                Task { @MainActor in
                    me.callback?()
                }
                return noErr
            },
            1,
            &spec,
            userData,
            &handler
        )

        if status != noErr {
            NSLog("HotkeyManager: failed to install handler, status=\(status)")
        }
    }

    func unregister() {
        if let hotKeyRef {
            UnregisterEventHotKey(hotKeyRef)
            self.hotKeyRef = nil
        }
        if let handler {
            RemoveEventHandler(handler)
            self.handler = nil
        }
        callback = nil
    }
}
