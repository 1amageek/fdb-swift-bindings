import DatabaseTypes

extension FDB {
    /// One owner-backed row returned by a FoundationDB range read.
    ///
    /// The key and value may share the native future that owns their storage.
    /// Call `copyBytes()` only when either payload must outlive that ownership
    /// boundary independently.
    public struct KeyValue: Sendable, Hashable {
        public let key: ByteString
        public let value: ByteString

        public init(key: ByteString, value: ByteString) {
            self.key = key
            self.value = value
        }
    }
}
