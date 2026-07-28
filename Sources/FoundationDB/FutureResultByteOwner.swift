import DatabaseTypes

/// Owns one immutable byte range returned by a FoundationDB future.
///
/// FoundationDB keeps result memory valid until the source future is destroyed.
/// This owner retains that future and exposes the native range without copying.
final class FutureResultByteOwner: ByteStringOwner {
    private let byteAddress: UInt
    let count: Int
    private let future: FutureOwner

    /// A field can retain an entire FoundationDB result batch through the
    /// future. The C API does not expose that allocation's total byte count.
    var retainedByteCount: Int? { nil }

    init(
        sourceBytes: UnsafePointer<UInt8>?,
        count: Int,
        future: FutureOwner
    ) {
        precondition(count >= 0)
        precondition(count == 0 || sourceBytes != nil)
        self.byteAddress = sourceBytes.map(UInt.init(bitPattern:)) ?? 0
        self.count = count
        self.future = future
    }

    func borrowBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try withExtendedLifetime(future) {
            guard count > 0 else {
                return try body(
                    UnsafeRawBufferPointer(start: nil, count: 0)
                )
            }
            guard let bytes = UnsafeRawPointer(bitPattern: byteAddress) else {
                preconditionFailure(
                    "FoundationDB result byte address is invalid"
                )
            }
            return try body(
                UnsafeRawBufferPointer(start: bytes, count: count)
            )
        }
    }
}
