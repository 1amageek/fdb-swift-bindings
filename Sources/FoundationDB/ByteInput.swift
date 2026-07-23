/// Bytes borrowed synchronously for one FoundationDB input operation.
///
/// The pointer passed to `body` is valid only for the duration of that call.
/// FoundationDB input APIs consume the bytes before their C function returns,
/// so conforming values do not need to materialize an intermediate array.
/// A conforming type must invoke `body` exactly once. A nonempty buffer must
/// have a nonnull base address.
extension FDB {
    public protocol ByteInput: Sendable {
        func withUnsafeBytes<Result>(
            _ body: (UnsafeRawBufferPointer) throws -> Result
        ) rethrows -> Result
    }
}

extension Array: FDB.ByteInput where Element == UInt8 {}

extension String: FDB.ByteInput {
    public func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        if let result = try utf8.withContiguousStorageIfAvailable({ bytes in
            try body(UnsafeRawBufferPointer(bytes))
        }) {
            return result
        }

        // A noncontiguous String must be encoded at this API boundary. The
        // temporary owner remains alive for the complete synchronous borrow.
        let encoded = [UInt8](utf8)
        return try encoded.withUnsafeBytes(body)
    }
}

@inline(__always)
func withInputBytes<Source: FDB.ByteInput, Result>(
    _ source: Source,
    _ body: (UnsafePointer<UInt8>, Int32) throws -> Result
) throws -> Result {
    return try source.withUnsafeBytes { bytes in
        guard let length = Int32(exactly: bytes.count) else {
            throw FDB.InputError.byteCountOutOfRange(bytes.count)
        }

        if bytes.isEmpty {
            var sentinel: UInt8 = 0
            return try withUnsafePointer(to: &sentinel) {
                try body($0, length)
            }
        }

        guard let baseAddress = bytes.baseAddress else {
            throw FDB.InputError.missingByteAddress(count: bytes.count)
        }
        return try body(
            baseAddress.assumingMemoryBound(to: UInt8.self),
            length
        )
    }
}

@inline(__always)
func validatedParameter(_ value: Int, named parameter: String) throws -> Int32 {
    guard let result = Int32(exactly: value) else {
        throw FDB.InputError.integerOutOfRange(
            parameter: parameter,
            value: value
        )
    }
    return result
}
