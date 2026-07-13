// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "JuiceMount",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "JuiceMount", targets: ["JuiceMount"]),
        // QuickLook thumbnail appex executable. scripts/build-app.sh wraps it
        // into Contents/PlugIns/JuiceMountThumbnails.appex and signs it
        // (sandboxed) before the outer app — see that script.
        .executable(name: "JuiceMountThumbnails", targets: ["JuiceMountThumbnails"])
    ],
    dependencies: [
        // Sparkle 2 auto-updater. Distributed as a SwiftPM binary artifact
        // (Sparkle.framework). scripts/build-app.sh embeds the framework into
        // Contents/Frameworks/ and signs it (and its nested XPC services)
        // inside-out before signing the app — see that script for the ordering.
        .package(url: "https://github.com/sparkle-project/Sparkle", from: "2.6.0")
    ],
    targets: [
        .executableTarget(
            name: "JuiceMount",
            dependencies: [
                "JuiceMountCore",
                .product(name: "Sparkle", package: "Sparkle")
            ],
            path: "Sources/JuiceMount"
        ),
        .executableTarget(
            name: "JuiceMountThumbnails",
            path: "Sources/JuiceMountThumbnails",
            linkerSettings: [
                // Explicit so the appex never depends on Swift autolink
                // pulling these in. Foundation provides _NSExtensionMain
                // (the appex entry point main.swift trampolines into).
                .linkedFramework("Foundation"),
                .linkedFramework("QuickLookThumbnailing"),
                .linkedFramework("AppKit"),
                .linkedFramework("ImageIO")
            ]
        ),
        .target(
            name: "JuiceMountCore",
            path: "Sources/JuiceMountCore",
            publicHeadersPath: "include",
            cSettings: [
                .headerSearchPath("include")
            ]
            // Note: -L and -lnfsd flags are passed by scripts/build-app.sh at build time.
            // We don't embed them here so the package stays portable across environments.
        )
    ]
)
