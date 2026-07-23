extension FDB {
    /// A native result that violates the FoundationDB result contract.
    public enum ResultError: Error, Sendable, Equatable, CustomStringConvertible {
        case unexpectedByteCount(result: String, expected: Int, actual: Int)

        public var description: String {
            switch self {
            case .unexpectedByteCount(let result, let expected, let actual):
                return "\(result) contained \(actual) bytes; expected \(expected)"
            }
        }
    }
}
