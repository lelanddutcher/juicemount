import Foundation
import SwiftUI
import Darwin

// Registration-only command dispatch must run before SwiftUI constructs the
// App, its delegate, or any server/settings controller. Ordinary launches use
// the same SwiftUI entry point as before.
@main
@MainActor
enum AppEntryPoint {
    static func main() {
        let code = UnmountHelperCommand.dispatch(
            arguments: CommandLine.arguments,
            environment: ProcessInfo.processInfo.environment,
            makeDaemon: { SMAppServiceUnmountHelperDaemon() },
            appTrust: { UnmountHelperService.appTrust(of: Bundle.main.bundleURL, requirement: UnmountHelperCommand.appRequirement) },
            ping: { UnmountHelperService.xpcPing(timeout: 2) },
            write: { line in FileHandle.standardOutput.write(Data((line + "\n").utf8)) },
            launchGUI: { JuiceMountApp.main() })
        if let code { exit(Int32(code)) }
    }
}
