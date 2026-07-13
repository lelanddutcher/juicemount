//
//  main.swift
//  JuiceMountThumbnails
//
//  App-extension entry point. Xcode builds .appex binaries by linking with
//  `-e _NSExtensionMain`, so the process starts inside Foundation's
//  NSExtension machinery (which reads NSExtensionPrincipalClass from
//  Info.plist, instantiates it, and services extension XPC requests —
//  thumbnailsd never calls a regular main()). SwiftPM can only build a plain
//  executable, so this main.swift exists solely to hand control to that same
//  entry point. Foundation exports _NSExtensionMain on macOS; verified via
//  `dyld_info -exports .../Foundation | grep NSExtensionMain`.
//

import Foundation

@_silgen_name("NSExtensionMain")
private func NSExtensionMain(
    _ argc: Int32,
    _ argv: UnsafeMutablePointer<UnsafeMutablePointer<CChar>?>
) -> Int32

// Effectively never returns: NSExtensionMain runs the extension's XPC
// listener loop for the life of the process. exit() is belt-and-braces for
// the theoretical return path.
exit(NSExtensionMain(CommandLine.argc, CommandLine.unsafeArgv))
