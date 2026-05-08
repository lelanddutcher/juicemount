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
    targets: [
        .executableTarget(
            name: "JuiceMount",
            dependencies: ["JuiceMountCore"],
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
