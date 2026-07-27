import DatabaseTypes

extension FDB {
    /// The 10-byte version assigned to a committed transaction.
    ///
    /// This value is distinct from ``Versionstamp``, whose 12-byte tuple form
    /// also contains a two-byte user version.
    public struct TransactionVersionstamp: Sendable, Hashable {
        public static let byteCount = 10

        public let bytes: ByteString

        public init(bytes: ByteString) throws {
            guard bytes.count == Self.byteCount else {
                throw ResultError.unexpectedByteCount(
                    result: "transaction versionstamp",
                    expected: Self.byteCount,
                    actual: bytes.count
                )
            }
            self.bytes = bytes
        }
    }
}
