# FoundationDB Swift Bindings

Swift 6.4 bindings for FoundationDB with explicit lifetime, retry, and result-presence semantics.

## Byte ownership

The bindings use `DatabaseTypes.ByteString` as their immutable retained byte
value. FoundationDB future results retain the future that owns the native
result storage, so point reads, keys, values, and range rows are exposed without
copying their payload bytes.

`FDB.ByteInput` is a separate synchronous borrowing contract for request input.
`ByteString`, `[UInt8]`, and `String` can be passed directly. A key selector
retains an existing `ByteString` without copying; arbitrary mutable input is
copied once because the selector must store an immutable value.

## Quick Start

### Initialize the Client

```swift
import FoundationDB
import DatabaseTypes

// Initialize FoundationDB
try await FDBClient.initialize()
let database = try FDBClient.openDatabase()
```

### Basic Operations

```swift
// Simple key-value operations
try await database.withTransaction { transaction in
    // Set a value
    let key = "hello"
    let value = "world"
    try transaction.setValue([UInt8](value.utf8), for: [UInt8](key.utf8))

    // Get a value
    if let valueBytes = try await transaction.getValue(for: [UInt8](key.utf8)) {
        print(String(decoding: valueBytes, as: UTF8.self)) // "world"
    }

    // Delete a key
    try transaction.clear(key: [UInt8](key.utf8))
}
```

### Range Queries

```swift
// Efficient streaming over large result sets
let sequence = transaction.getRange(
    from: .firstGreaterOrEqual([UInt8]("user:".utf8)),
    to: .firstGreaterOrEqual([UInt8]("user;".utf8))
)

for try await row in sequence {
    let userId = String(decoding: row.key, as: UTF8.self)
    let userData = String(decoding: row.value, as: UTF8.self)
    // Process each key-value pair as it streams
}
```

### Atomic Operations

```swift
try await database.withTransaction { transaction in
    // Atomic increment
    let counterKey = "counter"
    let increment = withUnsafeBytes(of: Int64(1).littleEndian) { Array($0) }
    try transaction.atomicOp(
        key: [UInt8](counterKey.utf8),
        param: increment,
        mutationType: .add
    )
}
```

### Lifecycle Management

FDB resources are managed automatically via ARC and are safe at process exit.
For explicit shutdown (tests, graceful server termination):

```swift
// 1. Release all database references
database = nil  // triggers fdb_database_destroy via ARC

// 2. Stop the network
FDBClient.shutdown()  // fdb_stop_network + thread join
```

**Important**: `shutdown()` requires all `FDBDatabase` instances to be released first.
A `precondition` failure occurs if active databases remain.

If `shutdown()` is not called, the OS reclaims all resources at process exit.
This is safe — FoundationDB is transactional and the server handles client disconnects gracefully.

## Requirements

- Swift 6.4+
- FoundationDB 7.1+
- macOS 15+ / Linux

## Installation

Add the package to your `Package.swift`:

```swift
dependencies: [
    .package(
        url: "https://github.com/1amageek/fdb-swift-bindings.git",
        from: "0.3.1"
    )
]
```

## Documentation

For detailed API documentation and advanced usage patterns, see the inline documentation in the source files.

## License

Licensed under the Apache License, Version 2.0. See LICENSE for details.
