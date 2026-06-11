// svg2png.swift — tiny SVG → PNG rasterizer with zero dependencies.
//
// Modern macOS can load SVG natively through NSImage (ImageIO gained SVG
// support in macOS 13+), so we don't need librsvg/inkscape/cairo in the
// build chain. build-app.sh compiles this once per build (swiftc -O) and
// invokes the binary for every menu-bar state icon and every AppIcon
// iconset size. It also runs interpreted for one-off use:
//
//   swift scripts/svg2png.swift logos/state-fault.svg /tmp/t.png 36          (square)
//   swift scripts/svg2png.swift site/assets/og-card.svg /tmp/og.png 1200 630 (W × H)
//
// Exit codes: 0 ok, 1 usage, 2 input unloadable, 3 PNG encode failure.
// With one size arg the output is square (size × size) — the contract
// build-app.sh depends on. An optional fourth arg sets an explicit height
// for non-square exports (OG cards). Transparent background, drawn with
// aspect-fit so mismatched viewBoxes don't distort.

import AppKit

let args = CommandLine.arguments
guard args.count == 4 || args.count == 5,
      let width = Int(args[3]), width > 0,
      let height = (args.count == 5 ? Int(args[4]) : width), height > 0 else {
    FileHandle.standardError.write(Data("usage: svg2png <in.svg> <out.png> <width> [height]\n".utf8))
    exit(1)
}

guard let img = NSImage(contentsOfFile: args[1]), img.size.width > 0, img.size.height > 0 else {
    FileHandle.standardError.write(Data("svg2png: cannot load \(args[1])\n".utf8))
    exit(2)
}

guard let rep = NSBitmapImageRep(
    bitmapDataPlanes: nil,
    pixelsWide: width,
    pixelsHigh: height,
    bitsPerSample: 8,
    samplesPerPixel: 4,
    hasAlpha: true,
    isPlanar: false,
    colorSpaceName: .deviceRGB,
    bytesPerRow: 0,
    bitsPerPixel: 0
) else {
    FileHandle.standardError.write(Data("svg2png: cannot allocate bitmap\n".utf8))
    exit(3)
}

// Aspect-fit the source into the canvas.
let scale = min(CGFloat(width) / img.size.width, CGFloat(height) / img.size.height)
let drawSize = NSSize(width: img.size.width * scale, height: img.size.height * scale)
let origin = NSPoint(x: (CGFloat(width) - drawSize.width) / 2,
                     y: (CGFloat(height) - drawSize.height) / 2)

NSGraphicsContext.saveGraphicsState()
NSGraphicsContext.current = NSGraphicsContext(bitmapImageRep: rep)
img.draw(in: NSRect(origin: origin, size: drawSize),
         from: .zero, operation: .sourceOver, fraction: 1.0)
NSGraphicsContext.restoreGraphicsState()

guard let png = rep.representation(using: .png, properties: [:]) else {
    FileHandle.standardError.write(Data("svg2png: PNG encode failed\n".utf8))
    exit(3)
}
do {
    try png.write(to: URL(fileURLWithPath: args[2]))
} catch {
    FileHandle.standardError.write(Data("svg2png: write failed: \(error)\n".utf8))
    exit(3)
}
print("OK \(width)x\(height) \(args[2])")
