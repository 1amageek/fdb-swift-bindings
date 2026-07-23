/// An immutable FoundationDB byte string with retained ownership.
///
/// Array inputs use copy-on-write ownership. FoundationDB results retain the
/// future that owns their memory, so slicing does not copy payload bytes. Call
/// `copyBytes()` only at an explicit ownership boundary.
extension FDB {
public struct ByteString:
        ByteInput,
        Sendable,
        Hashable,
        RandomAccessCollection,
        ExpressibleByArrayLiteral {
    public typealias Element = UInt8
    public typealias Index = Int
    public typealias SubSequence = ByteString
    public typealias ArrayLiteralElement = UInt8

    private let owner: any ByteStringOwner
    private let byteRange: Range<Int>

    public init(_ bytes: FDB.Bytes) {
        let owner = ArrayByteStringOwner(bytes)
        self.owner = owner
        self.byteRange = 0..<owner.count
    }

    public init<Source: FDB.ByteInput>(copying source: Source) {
        let owner = source.withUnsafeBytes { bytes in
            precondition(
                bytes.isEmpty || bytes.baseAddress != nil,
                "Byte input returned no address for nonempty bytes"
            )
            return ArrayByteStringOwner(Array(bytes))
        }
        self.owner = owner
        self.byteRange = 0..<owner.count
    }

    public init(arrayLiteral elements: UInt8...) {
        self.init(elements)
    }

    private init(
        owner: any ByteStringOwner,
        byteRange: Range<Int>
    ) {
        precondition(
            byteRange.lowerBound >= 0
                && byteRange.upperBound <= owner.count
        )
        self.owner = owner
        self.byteRange = byteRange
    }

    init<Owner: Sendable>(
        sharing sourceBytes: UnsafePointer<UInt8>?,
        count: Int,
        retaining owner: Owner
    ) {
        let owner = RetainedByteStringOwner(
            sourceBytes: sourceBytes,
            count: count,
            owner: owner
        )
        self.owner = owner
        self.byteRange = 0..<count
    }

    public var startIndex: Int { 0 }
    public var endIndex: Int { byteRange.count }

    public subscript(position: Int) -> UInt8 {
        precondition(indices.contains(position))
        return withUnsafeBytes { $0[position] }
    }

    public subscript(bounds: Range<Int>) -> ByteString {
        precondition(
            bounds.lowerBound >= startIndex && bounds.upperBound <= endIndex
        )
        return ByteString(
            owner: owner,
            byteRange: (byteRange.lowerBound + bounds.lowerBound)..<(
                byteRange.lowerBound + bounds.upperBound
            )
        )
    }

    public func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        var outcome: ByteAccessOutcome<Result> = .missing
        try owner.withBytes { bytes in
            guard case .missing = outcome else {
                preconditionFailure("Byte string owner invoked its callback more than once")
            }
            precondition(
                bytes.count == owner.count,
                "Byte string owner returned an inconsistent count"
            )
            precondition(
                bytes.isEmpty || bytes.baseAddress != nil,
                "Byte string owner returned no address for nonempty bytes"
            )
            outcome = .value(try body(UnsafeRawBufferPointer(
                rebasing: bytes[byteRange]
            )))
        }

        switch outcome {
        case .missing:
            preconditionFailure("Byte string owner did not invoke its callback")
        case .value(let result):
            return result
        }
    }

    public func copyBytes() -> FDB.Bytes {
        withUnsafeBytes { Array($0) }
    }

    public static func == (lhs: ByteString, rhs: ByteString) -> Bool {
        guard lhs.count == rhs.count else { return false }
        return lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                lhsBytes.elementsEqual(rhsBytes)
            }
        }
    }

    public static func == (lhs: ByteString, rhs: FDB.Bytes) -> Bool {
        guard lhs.count == rhs.count else { return false }
        return lhs.withUnsafeBytes { lhsBytes in
            rhs.withUnsafeBytes { rhsBytes in
                lhsBytes.elementsEqual(rhsBytes)
            }
        }
    }

    public static func == (lhs: FDB.Bytes, rhs: ByteString) -> Bool {
        rhs == lhs
    }

    public func hash(into hasher: inout Hasher) {
        withUnsafeBytes { bytes in
            for byte in bytes {
                hasher.combine(byte)
            }
        }
    }
}
}

private protocol ByteStringOwner: Sendable {
    var count: Int { get }

    func withBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows
}

private struct ArrayByteStringOwner: ByteStringOwner {
    let bytes: FDB.Bytes

    init(_ bytes: FDB.Bytes) {
        self.bytes = bytes
    }

    var count: Int { bytes.count }

    func withBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try bytes.withUnsafeBytes(body)
    }
}

private struct RetainedByteStringOwner<Owner: Sendable>: ByteStringOwner {
    let byteAddress: UInt
    let count: Int
    let owner: Owner

    init(
        sourceBytes: UnsafePointer<UInt8>?,
        count: Int,
        owner: Owner
    ) {
        precondition(count >= 0)
        precondition(count == 0 || sourceBytes != nil)
        self.byteAddress = sourceBytes.map(UInt.init(bitPattern:)) ?? 0
        self.count = count
        self.owner = owner
    }

    func withBytes(
        _ body: (UnsafeRawBufferPointer) throws -> Void
    ) rethrows {
        try withExtendedLifetime(owner) {
            if count == 0 {
                return try body(UnsafeRawBufferPointer(start: nil, count: 0))
            }
            guard let bytes = UnsafeRawPointer(bitPattern: byteAddress) else {
                preconditionFailure("Retained byte string address is invalid")
            }
            try body(UnsafeRawBufferPointer(start: bytes, count: count))
        }
    }
}

private enum ByteAccessOutcome<Value> {
    case missing
    case value(Value)
}
