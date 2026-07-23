extension FDB {
/// Invalid input rejected before a request reaches FoundationDB.
public enum InputError: Error, Sendable, Equatable, CustomStringConvertible {
    case byteCountOutOfRange(Int)
    case integerOutOfRange(parameter: String, value: Int)
    case missingByteAddress(count: Int)

    public var description: String {
        switch self {
        case .byteCountOutOfRange(let count):
            return "Byte count \(count) cannot be represented by the FoundationDB C API"
        case .integerOutOfRange(let parameter, let value):
            return "\(parameter) value \(value) cannot be represented by the FoundationDB C API"
        case .missingByteAddress(let count):
            return "Byte input returned no address for \(count) bytes"
        }
    }
}
}
