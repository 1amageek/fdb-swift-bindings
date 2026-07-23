import Synchronization
import Testing

@testable import FoundationDB

@Suite("FoundationDB byte-string ownership")
struct ByteStringOwnershipTests {
    @Test("Copying an input creates a stable value whose slices share ownership")
    func copiedInputIsStableAndSlicesShareOwnership() throws {
        let releaseRecorder = ByteInputReleaseRecorder()
        var source: BorrowTrackingByteInput? = BorrowTrackingByteInput(
            bytes: [0x10, 0x20, 0x30, 0x40, 0x50],
            releaseRecorder: releaseRecorder
        )
        var bytes: FDB.ByteString? = FDB.ByteString(
            copying: try #require(source)
        )

        #expect(source?.borrowCount == 1)
        source = nil
        #expect(releaseRecorder.wasReleased)

        let rootAddress = try #require(bytes).withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        var slice: FDB.ByteString? = try #require(bytes)[2..<5]
        let sliceAddress = try #require(slice).withUnsafeBytes {
            try #require($0.baseAddress.map(UInt.init(bitPattern:)))
        }
        #expect(sliceAddress == rootAddress + 2)
        #expect(try #require(slice).copyBytes() == [0x30, 0x40, 0x50])

        bytes = nil
        #expect(try #require(slice).copyBytes() == [0x30, 0x40, 0x50])
        slice = nil
    }

    @Test("Mutable input cannot change a retained byte string or its hash")
    func mutableInputCannotChangeRetainedValue() {
        let source = MutableByteInput(bytes: [0x10, 0x20, 0x30])
        let bytes = FDB.ByteString(copying: source)
        let values: Set<FDB.ByteString> = [bytes]

        source.replace(with: [0x90])

        #expect(bytes.copyBytes() == [0x10, 0x20, 0x30])
        #expect(values.contains(bytes))
    }

    @Test("Key selectors snapshot mutable input")
    func keySelectorSnapshotsMutableInput() {
        let source = MutableByteInput(bytes: [0x01, 0x02])
        let selector = FDB.KeySelector.firstGreaterOrEqual(source)

        source.replace(with: [0xff])

        #expect(selector.key.copyBytes() == [0x01, 0x02])
    }

    @Test("Empty input remains a valid FoundationDB argument")
    func emptyInputIsAValidFoundationDBArgument() throws {
        let address = try withInputBytes([UInt8]()) { bytes, length in
            #expect(length == 0)
            return UInt(bitPattern: bytes)
        }

        #expect(address != 0)
    }

    @Test("Oversized input fails inside its single borrow")
    func oversizedInputFailsInsideBorrow() {
        let byteCount = Int(Int32.max) + 1
        let source = OversizedByteInput(byteCount: byteCount)

        #expect(throws: FDB.InputError.byteCountOutOfRange(byteCount)) {
            try withInputBytes(source) { _, _ in
                Issue.record("Oversized input reached the C-call boundary")
            }
        }
        #expect(source.borrowCount == 1)
    }

    @Test("Out-of-range integer parameters fail deterministically")
    func integerRangeFailureIsTyped() {
        let value = Int(Int32.max) + 1

        #expect(throws: FDB.InputError.integerOutOfRange(
            parameter: "limit",
            value: value
        )) {
            try validatedParameter(value, named: "limit")
        }
    }
}

private final class BorrowTrackingByteInput: FDB.ByteInput, Sendable {
    private struct State: Sendable {
        var borrowCount = 0
    }

    private let bytes: [UInt8]
    private let state = Mutex(State())
    private let releaseRecorder: ByteInputReleaseRecorder

    init(bytes: [UInt8], releaseRecorder: ByteInputReleaseRecorder) {
        self.bytes = bytes
        self.releaseRecorder = releaseRecorder
    }

    deinit {
        releaseRecorder.recordRelease()
    }

    var borrowCount: Int { state.withLock { $0.borrowCount } }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        state.withLock { $0.borrowCount += 1 }
        return try bytes.withUnsafeBytes(body)
    }
}

private final class MutableByteInput: FDB.ByteInput, Sendable {
    private let bytes: Mutex<[UInt8]>

    init(bytes: [UInt8]) {
        self.bytes = Mutex(bytes)
    }

    func replace(with bytes: [UInt8]) {
        self.bytes.withLock { $0 = bytes }
    }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let snapshot = bytes.withLock { $0 }
        return try snapshot.withUnsafeBytes(body)
    }
}

private final class ByteInputReleaseRecorder: Sendable {
    private let state = Mutex(false)

    var wasReleased: Bool { state.withLock { $0 } }

    func recordRelease() {
        state.withLock { $0 = true }
    }
}

private final class OversizedByteInput: FDB.ByteInput, Sendable {
    let byteCount: Int
    private let state = Mutex(0)

    init(byteCount: Int) {
        self.byteCount = byteCount
    }

    var borrowCount: Int { state.withLock { $0 } }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        state.withLock { $0 += 1 }
        return try body(UnsafeRawBufferPointer(
            start: UnsafeRawPointer(bitPattern: 1),
            count: byteCount
        ))
    }
}
