// swift-tools-version:5.9
import PackageDescription

let package = Package(
    name: "JuiceMount",
    platforms: [
        .macOS(.v14)
    ],
    products: [
        .executable(name: "JuiceMount", targets: ["JuiceMount"])
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
