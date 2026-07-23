/*
 * Database.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2016-2025 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import CFoundationDB
import Synchronization

/// A FoundationDB database connection.
///
/// `FDBDatabase` represents a connection to a FoundationDB database and implements
/// the `DatabaseProtocol` protocol. It provides transaction creation capabilities and
/// automatically manages the underlying database connection resource.
///
/// ## Usage Example
/// ```swift
/// let database = try FDBClient.openDatabase()
/// let transaction = try database.createTransaction()
/// ```
public final class FDBDatabase: DatabaseProtocol, Sendable {
    /// The underlying FoundationDB database pointer.
    private let database: Mutex<UInt?>

    /// Initializes a new database instance with the given database pointer.
    ///
    /// - Parameter database: The underlying FoundationDB database pointer.
    init(database: OpaquePointer) {
        self.database = Mutex(UInt(bitPattern: database))
        FDBNetwork.shared.trackDatabase()
    }

    /// Cleans up the database connection when the instance is deallocated.
    ///
    /// Destroys the underlying C database handle first, then notifies
    /// `FDBNetwork` that this database is no longer active.
    deinit {
        database.withLock { storedAddress in
            if let address = storedAddress,
               let database = OpaquePointer(bitPattern: address) {
                fdb_database_destroy(database)
                storedAddress = nil
            }
        }
        FDBNetwork.shared.releaseDatabase()
    }

    /// Creates a new transaction for database operations.
    ///
    /// Creates and returns a new transaction that can be used to perform
    /// read and write operations on the database.
    ///
    /// - Returns: A new transaction instance conforming to `TransactionProtocol`.
    /// - Throws: `FDBError` if the transaction cannot be created.
    public func createTransaction() throws -> FDBTransaction {
        let result: (error: Int32, address: UInt?) = database.withLock { address in
            guard let address, let database = OpaquePointer(bitPattern: address) else {
                return (FDBErrorCode.internalError.rawValue, nil)
            }
            var transaction: OpaquePointer?
            let error = fdb_database_create_transaction(database, &transaction)
            return (error, transaction.map(UInt.init(bitPattern:)))
        }
        if result.error != 0 {
            throw FDBError(code: result.error)
        }

        guard let address = result.address,
              let transaction = OpaquePointer(bitPattern: address) else {
            throw FDBError(.internalError)
        }

        return FDBTransaction(transaction: transaction)
    }

    /// Sets a database option with a byte array value.
    ///
    /// - Parameters:
    ///   - value: The value for the option (optional).
    ///   - option: The database option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    public func setOption<Value: FDB.ByteInput>(
        to value: Value,
        forOption option: FDB.DatabaseOption
    ) throws {
        let error = try withInputBytes(value) { bytes, length in
            database.withLock { address -> Int32 in
                guard let address,
                      let database = OpaquePointer(bitPattern: address) else {
                    return FDBErrorCode.internalError.rawValue
                }
                return fdb_database_set_option(
                    database,
                    FDBDatabaseOption(option.rawValue),
                    bytes,
                    length
                )
            }
        }

        if error != 0 {
            throw FDBError(code: error)
        }
    }

    public func setOption(forOption option: FDB.DatabaseOption) throws {
        let error = database.withLock { address -> Int32 in
            guard let address,
                  let database = OpaquePointer(bitPattern: address) else {
                return FDBErrorCode.internalError.rawValue
            }
            return fdb_database_set_option(
                database,
                FDBDatabaseOption(option.rawValue),
                nil,
                0
            )
        }

        if error != 0 {
            throw FDBError(code: error)
        }
    }

    /// Sets a database option with a string value.
    ///
    /// - Parameters:
    ///   - value: The string value for the option.
    ///   - option: The database option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    public func setOption(to value: String, forOption option: FDB.DatabaseOption) throws {
        try setOption(to: Array(value.utf8), forOption: option)
    }

    /// Sets a database option with an integer value.
    ///
    /// - Parameters:
    ///   - value: The integer value for the option.
    ///   - option: The database option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    public func setOption(to value: Int, forOption option: FDB.DatabaseOption) throws {
        var val = Int64(value).littleEndian
        try withUnsafeBytes(of: &val) { bytes in
            try setOption(to: Array(bytes), forOption: option)
        }
    }
}
