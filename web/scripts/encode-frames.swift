// Encodes the frames record-demo.mjs captured into an H.264 MP4, using only
// macOS system frameworks (AVFoundation, ImageIO).
//
//   swiftc -O web/scripts/encode-frames.swift -o /tmp/encode-frames
//   /tmp/encode-frames <frames-dir> <out.mp4>
//
// <frames-dir>/frames.json lists {file, t} with t in seconds. Chrome's
// screencast only emits a frame when the page changes, so the output is
// resampled to a constant 30 fps, each output frame showing the newest
// captured frame at or before its time. Nothing is interpolated or invented.

import AVFoundation
import Foundation
import ImageIO

struct Frame: Decodable {
    let file: String
    let t: Double
}

let args = CommandLine.arguments
guard args.count == 3 else {
    FileHandle.standardError.write("usage: encode-frames <frames-dir> <out.mp4>\n".data(using: .utf8)!)
    exit(2)
}
let dir = URL(fileURLWithPath: args[1])
let out = URL(fileURLWithPath: args[2])
let fps: Int32 = 30

let frames = try JSONDecoder().decode([Frame].self, from: Data(contentsOf: dir.appendingPathComponent("frames.json")))
guard let first = frames.first, let last = frames.last else {
    FileHandle.standardError.write("no frames\n".data(using: .utf8)!)
    exit(1)
}

func load(_ f: Frame) -> CGImage {
    let url = dir.appendingPathComponent(f.file) as CFURL
    guard let src = CGImageSourceCreateWithURL(url, nil), let img = CGImageSourceCreateImageAtIndex(src, 0, nil) else {
        fatalError("cannot decode \(f.file)")
    }
    return img
}

let firstImage = load(first)
// H.264 needs even dimensions.
let width = firstImage.width & ~1
let height = firstImage.height & ~1

try? FileManager.default.removeItem(at: out)
let writer = try AVAssetWriter(outputURL: out, fileType: .mp4)
let input = AVAssetWriterInput(mediaType: .video, outputSettings: [
    AVVideoCodecKey: AVVideoCodecType.h264,
    AVVideoWidthKey: width,
    AVVideoHeightKey: height,
    AVVideoCompressionPropertiesKey: [
        AVVideoAverageBitRateKey: 3_000_000,
        AVVideoProfileLevelKey: AVVideoProfileLevelH264HighAutoLevel,
        AVVideoMaxKeyFrameIntervalKey: 60,
    ],
])
input.expectsMediaDataInRealTime = false
let adaptor = AVAssetWriterInputPixelBufferAdaptor(assetWriterInput: input, sourcePixelBufferAttributes: [
    kCVPixelBufferPixelFormatTypeKey as String: kCVPixelFormatType_32ARGB,
    kCVPixelBufferWidthKey as String: width,
    kCVPixelBufferHeightKey as String: height,
])
writer.add(input)
guard writer.startWriting() else { fatalError("cannot start writing: \(String(describing: writer.error))") }
writer.startSession(atSourceTime: .zero)

func pixelBuffer(_ image: CGImage) -> CVPixelBuffer {
    var pb: CVPixelBuffer?
    CVPixelBufferPoolCreatePixelBuffer(nil, adaptor.pixelBufferPool!, &pb)
    let buffer = pb!
    CVPixelBufferLockBaseAddress(buffer, [])
    let ctx = CGContext(
        data: CVPixelBufferGetBaseAddress(buffer), width: width, height: height, bitsPerComponent: 8,
        bytesPerRow: CVPixelBufferGetBytesPerRow(buffer), space: CGColorSpaceCreateDeviceRGB(),
        bitmapInfo: CGImageAlphaInfo.noneSkipFirst.rawValue)!
    ctx.interpolationQuality = .high
    ctx.draw(image, in: CGRect(x: 0, y: 0, width: width, height: height))
    CVPixelBufferUnlockBaseAddress(buffer, [])
    return buffer
}

let t0 = first.t
let duration = last.t - t0 + 1.5  // hold the final frame briefly
let total = Int(duration * Double(fps))
var index = 0
var current = pixelBuffer(firstImage)
for n in 0..<total {
    let t = Double(n) / Double(fps)
    var next = index
    while next + 1 < frames.count && frames[next + 1].t - t0 <= t { next += 1 }
    if next != index {
        index = next
        current = pixelBuffer(load(frames[index]))
    }
    while !input.isReadyForMoreMediaData { Thread.sleep(forTimeInterval: 0.002) }
    if !adaptor.append(current, withPresentationTime: CMTime(value: CMTimeValue(n), timescale: fps)) {
        fatalError("append failed at frame \(n): \(String(describing: writer.error))")
    }
}
input.markAsFinished()
let done = DispatchSemaphore(value: 0)
writer.finishWriting { done.signal() }
done.wait()
guard writer.status == .completed else { fatalError("encoding failed: \(String(describing: writer.error))") }
print("wrote \(out.path): \(total) frames, \(String(format: "%.1f", duration)) s, from \(frames.count) captured")
