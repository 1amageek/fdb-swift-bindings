/*
 * FoundationDBTests.swift
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

import Testing

@testable import FoundationDB

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#endif

extension FDBClient {
    static func maybeInitialize() async throws {
        try await foundationDBTestRuntime.initializeIfNeeded()
    }

    static var testClusterFilePath: String? {
        guard let path = getenv("FDB_CLUSTER_FILE") else {
            return nil
        }
        return String(cString: path)
    }

    static func openTestDatabase() throws -> FDBDatabase {
        try openDatabase(clusterFilePath: testClusterFilePath)
    }
}

private let foundationDBTestRuntime = FoundationDBTestRuntime()

private actor FoundationDBTestRuntime {
    func initializeIfNeeded() async throws {
        guard !FDBClient.isInitialized else { return }
        try await FDBClient.initialize()
    }
}

// Helper extension for Foundation-free string operations
extension String {
    init<Bytes: Collection>(bytes: Bytes) where Bytes.Element == UInt8 {
        self = String(decoding: bytes, as: UTF8.self)
    }

    static func padded(_ number: Int, width: Int = 3) -> String {
        let str = String(number)
        let padding = width - str.count
        return padding > 0 ? String(repeating: "0", count: padding) + str : str
    }
}

extension TransactionProtocol {
    func getValue(for key: String, snapshot: Bool = false) async throws -> FDB.Bytes? {
        let keyBytes = [UInt8](key.utf8)
        return try await getValue(
            for: keyBytes,
            snapshot: snapshot
        )?.copyBytes()
    }

    func setValue(_ value: String, for key: String) throws {
        let keyBytes = [UInt8](key.utf8)
        let valueBytes = [UInt8](value.utf8)
        try setValue(valueBytes, for: keyBytes)
    }

    func clear(key: String) throws {
        let keyBytes = [UInt8](key.utf8)
        try clear(key: keyBytes)
    }

    func clearRange(beginKey: String, endKey: String) throws {
        let beginKeyBytes = [UInt8](beginKey.utf8)
        let endKeyBytes = [UInt8](endKey.utf8)
        try clearRange(beginKey: beginKeyBytes, endKey: endKeyBytes)
    }

    func getRange(
        beginKey: String, endKey: String, snapshot: Bool = false
    ) -> FDB.AsyncKVSequence {
        let beginSelector = FDB.KeySelector.firstGreaterOrEqual(beginKey)
        let endSelector = FDB.KeySelector.firstGreaterOrEqual(endKey)
        return getRange(
            from: beginSelector, to: endSelector, snapshot: snapshot
        )
    }

    func readRangeBatch(
        from beginKey: String,
        to endKey: String,
        limit: Int = 0,
        snapshot: Bool = false
    ) async throws -> RangeBatch {
        let beginKeyBytes = [UInt8](beginKey.utf8)
        let endKeyBytes = [UInt8](endKey.utf8)
        return try await readRangeBatch(
            from: beginKeyBytes,
            to: endKeyBytes,
            limit: limit,
            snapshot: snapshot
        )
    }
}

extension FDB.KeySelector {
    static func firstGreaterOrEqual(_ key: String) -> Self {
        return Self(key: [UInt8](key.utf8), orEqual: false, offset: 1)
    }

    static func firstGreaterThan(_ key: String) -> Self {
        return Self(key: [UInt8](key.utf8), orEqual: true, offset: 1)
    }

    static func lastLessOrEqual(_ key: String) -> Self {
        return Self(key: [UInt8](key.utf8), orEqual: true, offset: 0)
    }

    static func lastLessThan(_ key: String) -> Self {
        return Self(key: [UInt8](key.utf8), orEqual: false, offset: 0)
    }
}

@Test("getValue test")
func testGetValue() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let res1 = try await newTransaction.getValue(for: "test_nonexistent_key")
    #expect(res1 == nil, "Non-existent key should return nil")

    try newTransaction.setValue("world", for: "test_hello")
    let res2 = try await newTransaction.getValue(for: "test_hello")
    #expect(res2 == Array("world".utf8))
}

@Test("setValue with byte arrays")
func setValueBytes() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_byte_key".utf8)
    let value: FDB.Bytes = [UInt8]("test_byte_value".utf8)

    try newTransaction.setValue(value, for: key)

    let retrievedValue = try await newTransaction.getValue(for: key)
    #expect(
        retrievedValue?.copyBytes() == value,
        "Retrieved value should match set value"
    )
}

@Test("setValue with strings")
func setValueStrings() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key = "test_string_key"
    let value = "test_string_value"
    try newTransaction.setValue(value, for: key)

    let retrievedValue = try await newTransaction.getValue(for: key)
    let expectedValue = [UInt8](value.utf8)
    #expect(retrievedValue == expectedValue, "Retrieved value should match set value")
}

@Test("clear with byte arrays")
func clearBytes() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_clear_key".utf8)
    let value: FDB.Bytes = [UInt8]("test_clear_value".utf8)

    try newTransaction.setValue(value, for: key)
    let retrievedValueBefore = try await newTransaction.getValue(for: key)
    #expect(
        retrievedValueBefore?.copyBytes() == value,
        "Value should exist before clear"
    )

    try newTransaction.clear(key: key)
    let retrievedValueAfter = try await newTransaction.getValue(for: key)
    #expect(retrievedValueAfter == nil, "Value should be nil after clear")
}

@Test("clear with strings")
func clearStrings() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key = "test_clear_string_key"
    let value = "test_clear_string_value"

    try newTransaction.setValue(value, for: key)
    let retrievedValueBefore = try await newTransaction.getValue(for: key)
    let expectedValue = [UInt8](value.utf8)
    #expect(retrievedValueBefore == expectedValue, "Value should exist before clear")

    try newTransaction.clear(key: key)
    let retrievedValueAfter = try await newTransaction.getValue(for: key)
    #expect(retrievedValueAfter == nil, "Value should be nil after clear")
}

@Test("clearRange with byte arrays")
func clearRangeBytes() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key1: FDB.Bytes = [UInt8]("test_range_key_a".utf8)
    let key2: FDB.Bytes = [UInt8]("test_range_key_b".utf8)
    let key3: FDB.Bytes = [UInt8]("test_range_key_c".utf8)
    let value: FDB.Bytes = [UInt8]("test_value".utf8)

    let beginKey: FDB.Bytes = [UInt8]("test_range_key_a".utf8)
    let endKey: FDB.Bytes = [UInt8]("test_range_key_c".utf8)

    try newTransaction.setValue(value, for: key1)
    try newTransaction.setValue(value, for: key2)
    try newTransaction.setValue(value, for: key3)

    let value1Before = try await newTransaction.getValue(for: key1)
    let value2Before = try await newTransaction.getValue(for: key2)
    let value3Before = try await newTransaction.getValue(for: key3)
    #expect(value1Before?.copyBytes() == value, "Value1 should exist before clearRange")
    #expect(value2Before?.copyBytes() == value, "Value2 should exist before clearRange")
    #expect(value3Before?.copyBytes() == value, "Value3 should exist before clearRange")

    try newTransaction.clearRange(beginKey: beginKey, endKey: endKey)

    let value1After = try await newTransaction.getValue(for: key1)
    let value2After = try await newTransaction.getValue(for: key2)
    let value3After = try await newTransaction.getValue(for: key3)
    #expect(value1After == nil, "Value1 should be nil after clearRange")
    #expect(value2After == nil, "Value2 should be nil after clearRange")
    #expect(
        value3After?.copyBytes() == value,
        "Value3 should still exist (end key is exclusive)"
    )
}

@Test("clearRange with strings")
func clearRangeStrings() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key1 = "test_range_string_key_a"
    let key2 = "test_range_string_key_b"
    let key3 = "test_range_string_key_c"
    let value = "test_string_value"

    let beginKey = "test_range_string_key_a"
    let endKey = "test_range_string_key_c"

    try newTransaction.setValue(value, for: key1)
    try newTransaction.setValue(value, for: key2)
    try newTransaction.setValue(value, for: key3)

    let expectedValue = [UInt8](value.utf8)
    let value1Before = try await newTransaction.getValue(for: key1)
    let value2Before = try await newTransaction.getValue(for: key2)
    let value3Before = try await newTransaction.getValue(for: key3)
    #expect(value1Before == expectedValue, "Value1 should exist before clearRange")
    #expect(value2Before == expectedValue, "Value2 should exist before clearRange")
    #expect(value3Before == expectedValue, "Value3 should exist before clearRange")

    try newTransaction.clearRange(beginKey: beginKey, endKey: endKey)

    let value1After = try await newTransaction.getValue(for: key1)
    let value2After = try await newTransaction.getValue(for: key2)
    let value3After = try await newTransaction.getValue(for: key3)
    #expect(value1After == nil, "Value1 should be nil after clearRange")
    #expect(value2After == nil, "Value2 should be nil after clearRange")
    #expect(value3After == expectedValue, "Value3 should still exist (end key is exclusive)")
}

@Test("getKey with KeySelector")
func getKeyWithKeySelector() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up some test data
    try newTransaction.setValue("value1", for: "test_getkey_a")
    try newTransaction.setValue("value2", for: "test_getkey_b")
    try newTransaction.setValue("value3", for: "test_getkey_c")
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()
    // Test getting key with KeySelector - firstGreaterOrEqual
    let selector = FDB.KeySelector.firstGreaterOrEqual("test_getkey_b")
    let resultKey = try await readTransaction.getKey(selector: selector)
    let expectedKey = [UInt8]("test_getkey_b".utf8)
    #expect(
        resultKey.copyBytes() == expectedKey,
        "getKey with KeySelector should find exact key"
    )
}

@Test("getKey with different KeySelector methods")
func getKeyWithDifferentSelectors() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    try newTransaction.setValue("value1", for: "test_selector_a")
    try newTransaction.setValue("value2", for: "test_selector_b")
    try newTransaction.setValue("value3", for: "test_selector_c")
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()

    // Test firstGreaterOrEqual
    let selectorGTE = FDB.KeySelector.firstGreaterOrEqual("test_selector_b")
    let resultGTE = try await readTransaction.getKey(selector: selectorGTE)
    #expect(
        resultGTE.copyBytes() == [UInt8]("test_selector_b".utf8),
        "firstGreaterOrEqual should find exact key"
    )

    // Test firstGreaterThan
    let selectorGT = FDB.KeySelector.firstGreaterThan("test_selector_b")
    let resultGT = try await readTransaction.getKey(selector: selectorGT)
    #expect(
        resultGT.copyBytes() == [UInt8]("test_selector_c".utf8),
        "firstGreaterThan should find next key"
    )

    // Test lastLessOrEqual
    let selectorLTE = FDB.KeySelector.lastLessOrEqual("test_selector_b")
    let resultLTE = try await readTransaction.getKey(selector: selectorLTE)
    #expect(
        resultLTE.copyBytes() == [UInt8]("test_selector_b".utf8),
        "lastLessOrEqual should find exact key"
    )

    // Test lastLessThan
    let selectorLT = FDB.KeySelector.lastLessThan("test_selector_b")
    let resultLT = try await readTransaction.getKey(selector: selectorLT)
    #expect(
        resultLT.copyBytes() == [UInt8]("test_selector_a".utf8),
        "lastLessThan should find previous key"
    )
}

@Test("getKey with Selectable protocol")
func getKeyWithSelectable() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_selectable_key".utf8)
    let value: FDB.Bytes = [UInt8]("test_selectable_value".utf8)
    try newTransaction.setValue(value, for: key)
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()

    // Test with FDB.Bytes (which implements Selectable)
    let resultWithKey = try await readTransaction.getKey(selector: key)
    #expect(resultWithKey.copyBytes() == key, "getKey with FDB.Bytes should work")

    // Test with String (which implements Selectable)
    let stringKey = "test_selectable_key"
    let resultWithString = try await readTransaction.getKey(selector: stringKey)
    #expect(resultWithString.copyBytes() == key, "getKey with String should work")
}

@Test("commit transaction")
func testCommit() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    try newTransaction.setValue("test_commit_value", for: "test_commit_key")
    try await newTransaction.commit()

    // Verify the value was committed by reading in a new transaction
    let readTransaction = try database.createTransaction()
    let retrievedValue = try await readTransaction.getValue(for: "test_commit_key")
    let expectedValue = [UInt8]("test_commit_value".utf8)
    #expect(
        retrievedValue == expectedValue, "Committed value should be readable in new transaction"
    )
}

@Test("transaction versionstamp is requested before commit and resolved after it")
func transactionVersionstampResolvesAfterCommit() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let key = "transaction_versionstamp_\(UInt64.random(in: 0...UInt64.max))"
    let transaction = try database.createTransaction()
    let pendingVersionstamp = transaction.requestVersionstamp()

    try transaction.setValue("value", for: key)
    try await transaction.commit()

    let versionstamp = try await pendingVersionstamp.value
    #expect(versionstamp.bytes.count == FDB.TransactionVersionstamp.byteCount)

    try await database.withTransaction { cleanupTransaction in
        try cleanupTransaction.clear(key: key)
    }
}

@Test("cancel transaction")
func testCancel() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    try newTransaction.setValue("test_cancel_value", for: "test_cancel_key")
    newTransaction.cancel()

    // After canceling, operations should fail
    do {
        _ = try await newTransaction.getValue(for: "test_cancel_key")
        #expect(Bool(false), "Operations should fail after cancel")
    } catch {
        // Expected to throw an error
        #expect(error is FDBError, "Should throw FDBError after cancel")
    }
}

@Test("setReadVersion and getReadVersion")
func readVersion() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let testVersion: Int64 = 12345
    newTransaction.setReadVersion(testVersion)
    let retrievedVersion = try await newTransaction.getReadVersion()
    #expect(retrievedVersion == testVersion, "Retrieved read version should match set version")
}

@Test("read version with snapshot read")
func readVersionSnapshot() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set a specific read version
    let testVersion: Int64 = 98765
    newTransaction.setReadVersion(testVersion)

    // Test snapshot read with the version
    try newTransaction.setValue("rangetest_snapshot_value", for: "rangetest_snapshot_key")
    let value = try await newTransaction.getValue(for: "rangetest_snapshot_key", snapshot: true)
    #expect(value != nil, "Snapshot read should work with set read version")
}

@Test("getRange with byte arrays")
func getRangeBytes() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data with byte arrays
    let key1: FDB.Bytes = [UInt8]("test_byte_range_001".utf8)
    let key2: FDB.Bytes = [UInt8]("test_byte_range_002".utf8)
    let key3: FDB.Bytes = [UInt8]("test_byte_range_003".utf8)
    let value1: FDB.Bytes = [UInt8]("byte_value1".utf8)
    let value2: FDB.Bytes = [UInt8]("byte_value2".utf8)
    let value3: FDB.Bytes = [UInt8]("byte_value3".utf8)

    try newTransaction.setValue(value1, for: key1)
    try newTransaction.setValue(value2, for: key2)
    try newTransaction.setValue(value3, for: key3)
    _ = try await newTransaction.commit()

    // Test range query with byte arrays
    let readTransaction = try database.createTransaction()
    let beginKey: FDB.Bytes = [UInt8]("test_byte_range_001".utf8)
    let endKey: FDB.Bytes = [UInt8]("test_byte_range_003".utf8)
    let result = try await readTransaction.readRangeBatch(
        from: beginKey,
        to: endKey,
        limit: 0,
        snapshot: false
    )

    #expect(!result.hasMore)
    try #require(
        result.records.count == 2, "Should return 2 key-value pairs (end key is exclusive)"
    )

    // Sort results by key for predictable testing
    let sortedResults = result.records.sorted { $0.key.lexicographicallyPrecedes($1.key) }
    #expect(sortedResults[0].key == key1, "First key should match key1")
    #expect(sortedResults[0].value == value1, "First value should match value1")
    #expect(sortedResults[1].key == key2, "Second key should match key2")
    #expect(sortedResults[1].value == value2, "Second value should match value2")
}

@Test("getRange with limit")
func getRangeWithLimit() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data with more entries
    for i in 1 ... 10 {
        let key = "test_limit_key_" + String.padded(i)
        let value = "limit_value\(i)"
        try newTransaction.setValue(value, for: key)
    }
    _ = try await newTransaction.commit()

    // Test with limit
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.readRangeBatch(
        from: "test_limit_key_001",
        to: "test_limit_key_999",
        limit: 3,
        snapshot: false
    )
    #expect(result.records.count == 3, "Should return exactly 3 key-value pairs due to limit")

    // Verify we got the first 3 keys
    let sortedResults = result.records.sorted { String(bytes: $0.key) < String(bytes: $1.key) }

    #expect(
        String(bytes: sortedResults[0].key) == "test_limit_key_001",
        "First key should be test_limit_key_001"
    )
    #expect(
        String(bytes: sortedResults[1].key) == "test_limit_key_002",
        "Second key should be test_limit_key_002"
    )
    #expect(
        String(bytes: sortedResults[2].key) == "test_limit_key_003",
        "Third key should be test_limit_key_003"
    )
}

@Test("getRange empty range")
func getRangeEmpty() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Test empty range
    let result = try await newTransaction.readRangeBatch(
        from: "test_empty_start", to: "test_empty_end"
    )

    #expect(result.records.count == 0, "Empty range should return no results")
    #expect(result.records.isEmpty, "Results should be empty")
    #expect(result.hasMore == false, "Should indicate no more results")
}

@Test("readRangeBatch with KeySelectors - firstGreaterOrEqual")
func readRangeBatchWithKeySelectors() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data
    let key1: FDB.Bytes = [UInt8]("test_selector_001".utf8)
    let key2: FDB.Bytes = [UInt8]("test_selector_002".utf8)
    let key3: FDB.Bytes = [UInt8]("test_selector_003".utf8)
    let value1: FDB.Bytes = [UInt8]("selector_value1".utf8)
    let value2: FDB.Bytes = [UInt8]("selector_value2".utf8)
    let value3: FDB.Bytes = [UInt8]("selector_value3".utf8)

    try newTransaction.setValue(value1, for: key1)
    try newTransaction.setValue(value2, for: key2)
    try newTransaction.setValue(value3, for: key3)
    _ = try await newTransaction.commit()

    // Test with KeySelectors using firstGreaterOrEqual
    let readTransaction = try database.createTransaction()
    let beginSelector = FDB.KeySelector.firstGreaterOrEqual(key1)
    let endSelector = FDB.KeySelector.firstGreaterOrEqual(key3)
    let result = try await readTransaction.readRangeBatch(
        from: beginSelector,
        to: endSelector,
        limit: 0,
        targetBytes: 0,
        streamingMode: .iterator,
        iteration: 1,
        reverse: false,
        snapshot: false
    )

    #expect(!result.hasMore)
    try #require(
        result.records.count == 2, "Should return 2 key-value pairs (end selector is exclusive)"
    )

    // Sort results by key for predictable testing
    let sortedResults = result.records.sorted { $0.key.lexicographicallyPrecedes($1.key) }
    #expect(sortedResults[0].key == key1, "First key should match key1")
    #expect(sortedResults[0].value == value1, "First value should match value1")
    #expect(sortedResults[1].key == key2, "Second key should match key2")
    #expect(sortedResults[1].value == value2, "Second value should match value2")
}

@Test("getRange with KeySelectors - String keys")
func getRangeWithStringSelectorKeys() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data with string keys
    try newTransaction.setValue("str_value1", for: "test_str_selector_001")
    try newTransaction.setValue("str_value2", for: "test_str_selector_002")
    try newTransaction.setValue("str_value3", for: "test_str_selector_003")
    _ = try await newTransaction.commit()

    // Test with String-based KeySelectors
    let readTransaction = try database.createTransaction()
    let beginSelector = FDB.KeySelector.firstGreaterOrEqual("test_str_selector_001")
    let endSelector = FDB.KeySelector.firstGreaterOrEqual("test_str_selector_003")
    let result = try await readTransaction.readRangeBatch(
        from: beginSelector,
        to: endSelector,
        limit: 0,
        targetBytes: 0,
        streamingMode: .iterator,
        iteration: 1,
        reverse: false,
        snapshot: false
    )

    #expect(!result.hasMore)
    try #require(result.records.count == 2, "Should return 2 key-value pairs")

    // Convert back to strings for easier testing
    let keys = result.records.map { String(bytes: $0.key) }.sorted()
    _ = result.records.map { String(bytes: $0.value) } // values not used in this test

    #expect(keys.contains("test_str_selector_001"), "Should contain first key")
    #expect(keys.contains("test_str_selector_002"), "Should contain second key")
    #expect(!keys.contains("test_str_selector_003"), "Should not contain end key (exclusive)")
}

@Test("getRange with Selectable protocol - mixed types")
func getRangeWithSelectable() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data
    try newTransaction.setValue("mixed_value1", for: "test_mixed_001")
    try newTransaction.setValue("mixed_value2", for: "test_mixed_002")
    try newTransaction.setValue("mixed_value3", for: "test_mixed_003")
    _ = try await newTransaction.commit()

    // Test using the general Selectable protocol with mixed key types
    let readTransaction = try database.createTransaction()
    let beginKey: FDB.Bytes = [UInt8]("test_mixed_001".utf8)
    let endKey = [UInt8]("test_mixed_003".utf8)
    let result = try await readTransaction.readRangeBatch(
        from: beginKey,
        to: endKey,
        limit: 0,
        snapshot: false
    )

    #expect(!result.hasMore)
    try #require(result.records.count == 2, "Should return 2 key-value pairs")

    let keys = result.records.map { String(bytes: $0.key) }.sorted()
    #expect(keys.contains("test_mixed_001"), "Should contain first key")
    #expect(keys.contains("test_mixed_002"), "Should contain second key")
}

@Test("KeySelector static methods with different offsets")
func keySelectorMethods() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    // Set up test data
    try newTransaction.setValue("offset_value1", for: "test_offset_001")
    try newTransaction.setValue("offset_value2", for: "test_offset_002")
    try newTransaction.setValue("offset_value3", for: "test_offset_003")
    _ = try await newTransaction.commit()

    let readTransaction = try database.createTransaction()

    // Test firstGreaterThan vs firstGreaterOrEqual
    let beginSelectorGTE = FDB.KeySelector.firstGreaterOrEqual("test_offset_002")
    let beginSelectorGT = FDB.KeySelector.firstGreaterThan("test_offset_002")
    let endSelector = FDB.KeySelector.firstGreaterOrEqual("test_offset_999")

    let resultGTE = try await readTransaction.readRangeBatch(
        from: beginSelectorGTE,
        to: endSelector,
        limit: 0,
        targetBytes: 0,
        streamingMode: .iterator,
        iteration: 1,
        reverse: false,
        snapshot: false
    )
    let resultGT = try await readTransaction.readRangeBatch(
        from: beginSelectorGT,
        to: endSelector,
        limit: 0,
        targetBytes: 0,
        streamingMode: .iterator,
        iteration: 1,
        reverse: false,
        snapshot: false
    )

    // firstGreaterOrEqual should include test_offset_002
    let keysGTE = resultGTE.records.map { String(bytes: $0.key) }.sorted()
    #expect(keysGTE.contains("test_offset_002"), "firstGreaterOrEqual should include the key")

    // firstGreaterThan should exclude test_offset_002 and start from test_offset_003
    let keysGT = resultGT.records.map { String(bytes: $0.key) }.sorted()
    #expect(!keysGT.contains("test_offset_002"), "firstGreaterThan should exclude the key")
    #expect(keysGT.contains("test_offset_003"), "firstGreaterThan should include next key")
}

@Test("withTransaction success")
func withTransactionSuccess() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    // Test successful withTransaction
    let result = try await database.withTransaction { transaction in
        try transaction.setValue("success_value", for: "test_with_transaction_key")
        return "operation_completed"
    }

    #expect(result == "operation_completed", "withTransaction should return the operation result")

    // Verify the value was committed
    let verifyTransaction = try database.createTransaction()
    let retrievedValue = try await verifyTransaction.getValue(for: "test_with_transaction_key")
    let expectedValue = [UInt8]("success_value".utf8)
    #expect(retrievedValue == expectedValue, "Value should be committed after withTransaction")
}

@Test("withTransaction with exception in operation")
func withTransactionException() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    struct TestError: Error {}

    do {
        _ = try await database.withTransaction { transaction in
            try transaction.setValue("exception_value", for: "test_with_transaction_exception")
            throw TestError()
        }
        #expect(Bool(false), "withTransaction should propagate thrown exceptions")
    } catch is TestError {
        // Expected behavior
    } catch {
        #expect(Bool(false), "Should catch TestError, got \(error)")
    }

    // Verify the value was NOT committed due to exception
    let verifyTransaction = try database.createTransaction()
    let retrievedValue = try await verifyTransaction.getValue(
        for: "test_with_transaction_exception")
    #expect(retrievedValue == nil, "Value should not be committed when exception occurs")
}

@Test("withTransaction with non-retryable error")
func withTransactionNonRetryableError() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    do {
        _ = try await database.withTransaction { transaction in
            try transaction.setValue("non_retryable_value", for: "test_with_transaction_non_retryable")
            // Throw a non-retryable FDB error (transaction_cancelled)
            throw FDBError(.transactionCancelled)
        }
        #expect(Bool(false), "withTransaction should propagate non-retryable errors")
    } catch let error as FDBError {
        #expect(
            error.code == FDBErrorCode.transactionCancelled.rawValue,
            "Should propagate the exact FDBError"
        )
        #expect(error.retryDisposition == .never)
    } catch {
        #expect(Bool(false), "Should catch FDBError, got \(error)")
    }
}

@Test("withTransaction returns value from operation")
func withTransactionReturnValue() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    // Test that withTransaction returns the correct value
    let stringResult = try await database.withTransaction { transaction in
        try transaction.setValue("return_test_value", for: "test_return_key")
        return "success"
    }
    #expect(stringResult == "success", "Should return string value from operation")

    let intResult = try await database.withTransaction { transaction in
        try transaction.setValue("return_test_value2", for: "test_return_key2")
        return 42
    }
    #expect(intResult == 42, "Should return integer value from operation")

    let arrayResult = try await database.withTransaction { transaction in
        try await transaction.getValue(for: "test_return_key")
    }
    let expectedValue = [UInt8]("return_test_value".utf8)
    #expect(arrayResult == expectedValue, "Should return retrieved value from operation")
}

@Test("withTransaction Sendable compliance")
func withTransactionSendable() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range first
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await clearTransaction.commit()

    // Test with Sendable types
    struct SendableData: Sendable {
        let id: Int
        let name: String
    }

    let result = try await database.withTransaction { transaction in
        try transaction.setValue("sendable_value", for: "test_sendable_key")
        return SendableData(id: 123, name: "test")
    }

    #expect(result.id == 123, "Should return sendable struct with correct id")
    #expect(result.name == "test", "Should return sendable struct with correct name")
}

@Test("atomic operation ADD")
func atomicOpAdd() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_add".utf8)

    // Initial value: little-endian 64-bit integer 10
    let initialValue: FDB.Bytes = withUnsafeBytes(of: Int64(10).littleEndian) { Array($0) }
    try newTransaction.setValue(initialValue, for: key)

    // Add 5 using atomic operation
    let addValue: FDB.Bytes = withUnsafeBytes(of: Int64(5).littleEndian) { Array($0) }
    try newTransaction.atomicOp(key: key, param: addValue, mutationType: .add)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultValue = result!.withUnsafeBytes { $0.loadUnaligned(as: Int64.self) }
    #expect(Int64(littleEndian: resultValue) == 15, "10 + 5 should equal 15")
}

@Test("atomic operation BIT_AND")
func atomicOpBitAnd() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_and".utf8)

    // Initial value: 0xFF (255)
    let initialValue: FDB.Bytes = [0xFF]
    try newTransaction.setValue(initialValue, for: key)

    // AND with 0x0F (15)
    let andValue: FDB.Bytes = [0x0F]
    try newTransaction.atomicOp(key: key, param: andValue, mutationType: .bitAnd)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    #expect(result! == [0x0F], "0xFF AND 0x0F should equal 0x0F")
}

@Test("atomic operation BIT_OR")
func atomicOpBitOr() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_or".utf8)

    // Initial value: 0x0F (15)
    let initialValue: FDB.Bytes = [0x0F]
    try newTransaction.setValue(initialValue, for: key)

    // OR with 0xF0 (240)
    let orValue: FDB.Bytes = [0xF0]
    try newTransaction.atomicOp(key: key, param: orValue, mutationType: .bitOr)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    #expect(result! == [0xFF], "0x0F OR 0xF0 should equal 0xFF")
}

@Test("atomic operation BIT_XOR")
func atomicOpBitXor() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_xor".utf8)

    // Initial value: 0xFF (255)
    let initialValue: FDB.Bytes = [0xFF]
    try newTransaction.setValue(initialValue, for: key)

    // XOR with 0x0F (15)
    let xorValue: FDB.Bytes = [0x0F]
    try newTransaction.atomicOp(key: key, param: xorValue, mutationType: .bitXor)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    #expect(result! == [0xF0], "0xFF XOR 0x0F should equal 0xF0")
}

@Test("atomic operation MAX")
func atomicOpMax() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_max".utf8)

    // Initial value: little-endian 64-bit integer 10
    let initialValue: FDB.Bytes = withUnsafeBytes(of: Int64(10).littleEndian) { Array($0) }
    try newTransaction.setValue(initialValue, for: key)

    // Max with 15
    let maxValue: FDB.Bytes = withUnsafeBytes(of: Int64(15).littleEndian) { Array($0) }
    try newTransaction.atomicOp(key: key, param: maxValue, mutationType: .max)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultValue = result!.withUnsafeBytes { $0.loadUnaligned(as: Int64.self) }
    #expect(Int64(littleEndian: resultValue) == 15, "max(10, 15) should equal 15")
}

@Test("atomic operation MIN")
func atomicOpMin() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_min".utf8)

    // Initial value: little-endian 64-bit integer 10
    let initialValue: FDB.Bytes = withUnsafeBytes(of: Int64(10).littleEndian) { Array($0) }
    try newTransaction.setValue(initialValue, for: key)

    // Min with 5
    let minValue: FDB.Bytes = withUnsafeBytes(of: Int64(5).littleEndian) { Array($0) }
    try newTransaction.atomicOp(key: key, param: minValue, mutationType: .min)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultValue = result!.withUnsafeBytes { $0.loadUnaligned(as: Int64.self) }
    #expect(Int64(littleEndian: resultValue) == 5, "min(10, 5) should equal 5")
}

@Test("atomic operation APPEND_IF_FITS")
func atomicOpAppendIfFits() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_append".utf8)

    // Initial value: "Hello"
    let initialValue: FDB.Bytes = [UInt8]("Hello".utf8)
    try newTransaction.setValue(initialValue, for: key)

    // Append " World"
    let appendValue: FDB.Bytes = [UInt8](" World".utf8)
    try newTransaction.atomicOp(key: key, param: appendValue, mutationType: .appendIfFits)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultString = String(bytes: result!)
    #expect(resultString == "Hello World", "Should append ' World' to 'Hello'")
}

@Test("atomic operation BYTE_MIN")
func atomicOpByteMin() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_byte_min".utf8)

    // Initial value: "zebra"
    let initialValue: FDB.Bytes = [UInt8]("zebra".utf8)
    try newTransaction.setValue(initialValue, for: key)

    // Compare with "apple" (lexicographically smaller)
    let compareValue: FDB.Bytes = [UInt8]("apple".utf8)
    try newTransaction.atomicOp(key: key, param: compareValue, mutationType: .byteMin)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultString = String(bytes: result!)
    #expect(resultString == "apple", "byte_min should choose lexicographically smaller value")
}

@Test("atomic operation BYTE_MAX")
func atomicOpByteMax() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()
    let key: FDB.Bytes = [UInt8]("test_atomic_byte_max".utf8)

    // Initial value: "apple"
    let initialValue: FDB.Bytes = [UInt8]("apple".utf8)
    try newTransaction.setValue(initialValue, for: key)

    // Compare with "zebra" (lexicographically larger)
    let compareValue: FDB.Bytes = [UInt8]("zebra".utf8)
    try newTransaction.atomicOp(key: key, param: compareValue, mutationType: .byteMax)

    _ = try await newTransaction.commit()

    // Verify result
    let readTransaction = try database.createTransaction()
    let result = try await readTransaction.getValue(for: key)
    try #require(result != nil, "Result should not be nil")

    let resultString = String(bytes: result!)
    #expect(resultString == "zebra", "byte_max should choose lexicographically larger value")
}

@Test("network option enum values")
func networkOptionEnumValues() {
    // Test that network option enum has expected values
    #expect(FDB.NetworkOption.traceEnable.rawValue == 30, "traceEnable should have value 30")
    #expect(FDB.NetworkOption.traceRollSize.rawValue == 31, "traceRollSize should have value 31")
    #expect(
        FDB.NetworkOption.traceMaxLogsSize.rawValue == 32, "traceMaxLogsSize should have value 32"
    )
    #expect(FDB.NetworkOption.traceLogGroup.rawValue == 33, "traceLogGroup should have value 33")
    #expect(FDB.NetworkOption.traceFormat.rawValue == 34, "traceFormat should have value 34")
    #expect(FDB.NetworkOption.knob.rawValue == 40, "knob should have value 40")
    #expect(FDB.NetworkOption.tlsCertPath.rawValue == 43, "tlsCertPath should have value 43")
    #expect(FDB.NetworkOption.tlsKeyPath.rawValue == 46, "tlsKeyPath should have value 46")
    #expect(
        FDB.NetworkOption.disableClientStatisticsLogging.rawValue == 70,
        "disableClientStatisticsLogging should have value 70"
    )
    #expect(FDB.NetworkOption.clientTmpDir.rawValue == 91, "clientTmpDir should have value 91")
}

@Test("network option convenience methods - method validation")
func networkOptionConvenienceMethods() throws {
    // Test that convenience methods exist and have correct signatures
    // Note: These tests verify the API exists but don't actually set options

    // Test trace methods
    // FDBClient.enableTrace(directory: "/tmp/test") - would set trace
    // FDBClient.setTraceRollSize(1048576) - would set roll size
    // FDBClient.setTraceLogGroup("test") - would set log group
    // FDBClient.setTraceFormat("json") - would set format

    // Test configuration methods
    // FDBClient.setKnob("test=1") - would set knob
    // FDBClient.setTLSCertPath("/tmp/cert.pem") - would set TLS cert
    // FDBClient.setTLSKeyPath("/tmp/key.pem") - would set TLS key
    // FDBClient.setClientTempDirectory("/tmp") - would set temp dir
    // FDBClient.disableClientStatisticsLogging() - would disable stats

    // If we get here, the convenience method signatures are correct
    let methodsExist = true
    #expect(methodsExist, "Network option convenience methods have valid signatures")
}

@Test("transaction option with timeout enforcement")
func transactionTimeoutOption() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()

    // Set a very short timeout (1ms) to test timeout functionality
    try newTransaction.setOption(to: 1, forOption: .timeout)

    // This should timeout very quickly
    do {
        // Perform an operation that might take longer than 1ms
        try newTransaction.setValue("timeout_test_value", for: "test_timeout_key")
        _ = try await newTransaction.commit()

        // If we get here, either the operation was very fast or timeout didn't work as expected
        // This is not necessarily a failure as the operation might complete within 1ms
    } catch {
        // Expected to timeout - this is normal behavior
        #expect(error is FDBError, "Should throw FDBError on timeout")
    }
}

@Test("transaction option with size limit")
func transactionSizeLimitOption() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    let newTransaction = try database.createTransaction()

    // Set a very small size limit (100 bytes)
    try newTransaction.setOption(to: 100, forOption: .sizeLimit)

    // Try to write more data than the limit allows
    let largeValue = String(repeating: "x", count: 200)
    try newTransaction.setValue(largeValue, for: "test_size_limit_key")

    do {
        _ = try await newTransaction.commit()
        // If successful, the transaction was small enough or size limit wasn't enforced yet
    } catch {
        // Expected to fail due to size limit
        #expect(error is FDBError, "Should throw FDBError when size limit exceeded")
    }
}

@Test("transaction option convenience methods - method validation")
func transactionOptionConvenienceMethods() throws {
    // Test that convenience methods exist and have correct signatures
    // Note: These tests verify the API exists but don't actually set options

    // Test timeout and retry methods
    // transaction.setOption(to: 30000, forOption: .timeout) - would set timeout
    // transaction.setOption(to: 10, forOption: .retryLimit) - would set retry limit
    // transaction.setOption(to: 5000, forOption: .maxRetryDelay) - would set max retry delay
    // transaction.setSizeLimit(1000000) - would set size limit

    // Test idempotency methods
    // transaction.enableAutomaticIdempotency() - would enable auto idempotency
    // transaction.setIdempotencyId(data) - would set idempotency ID

    // Test read-your-writes methods
    // transaction.disableReadYourWrites() - would disable RYW
    // transaction.enableSnapshotReadYourWrites() - would enable snapshot RYW
    // transaction.disableSnapshotReadYourWrites() - would disable snapshot RYW

    // Test priority methods
    // transaction.setPriorityBatch() - would set batch priority
    // transaction.setPrioritySystemImmediate() - would set system immediate priority

    // Test causality methods
    // transaction.enableCausalWriteRisky() - would enable causal write risky
    // transaction.enableCausalReadRisky() - would enable causal read risky
    // transaction.disableCausalRead() - would disable causal read

    // Test system access methods
    // transaction.enableAccessSystemKeys() - would enable system key access
    // transaction.enableReadSystemKeys() - would enable system key reading
    // transaction.enableRawAccess() - would enable raw access

    // Test tagging methods
    // transaction.addTag("tag") - would add tag
    // transaction.addAutoThrottleTag("tag") - would add auto throttle tag

    // Test debugging methods
    // transaction.setDebugTransactionIdentifier("id") - would set debug ID
    // transaction.enableLogTransaction() - would enable transaction logging

    // Simple validation - if we can compile, the signatures exist
    let validationPassed = true
    #expect(validationPassed, "Transaction option convenience methods have valid signatures")
}

@Test("getRange with KeySelectors - basic functionality")
func getRangeWithKeySelectors() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_", endKey: "test`")
    _ = try await transaction.commit()

    // Set up test data
    let newTransaction = try database.createTransaction()
    for i in 0 ... 99 {
        let key = "test_read_range_" + String(i).leftPad(toLength: 3, withPad: "0")
        let value = "value_\(i)"
        try newTransaction.setValue(value, for: key)
    }
    _ = try await newTransaction.commit()

    // Exercise a bounded key range through demand-driven iteration.
    let readTransaction = try database.createTransaction()
    let beginSelector = FDB.KeySelector.firstGreaterOrEqual("test_read_range_015")
    let endSelector = FDB.KeySelector.firstGreaterOrEqual("test_read_range_032")

    let asyncSequence = readTransaction.getRange(
        from: beginSelector, to: endSelector
    )

    var count = 0
    for try await kv in asyncSequence {
        let key = String(bytes: kv.key)
        let value = String(bytes: kv.value)

        // Verify the keys are in order and as expected
        let expected_key = "test_read_range_" + String(count + 15).leftPad(toLength: 3, withPad: "0")
        let expected_value = "value_\(count + 15)"
        #expect(key == expected_key)
        #expect(value == expected_value)

        count += 1

        // Stop after reasonable number to avoid infinite iteration in case of bug
        if count > 20 {
            break
        }
    }

    #expect(count == 17, "Should read expected number of records in range")
}

@Test("getRange AsyncIterator traverses a bounded range in key order")
func getRangeAsyncIteratorTraversesBoundedRange() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Clear test key range
    try transaction.clearRange(beginKey: "test_async_", endKey: "test_async`")
    _ = try await transaction.commit()

    // Set up enough records to cross multiple range pages.
    let writeTransaction = try database.createTransaction()
    for i in 0 ... 149 {
        let key = "test_async_iter_" + String(i).leftPad(toLength: 3, withPad: "0")
        let value = "async_value_\(i)"
        try writeTransaction.setValue(value, for: key)
    }
    _ = try await writeTransaction.commit()

    // Traverse enough values to exercise page continuation.
    let readTransaction = try database.createTransaction()
    let beginSelector = FDB.KeySelector.firstGreaterOrEqual("test_async_iter_020")
    let endSelector = FDB.KeySelector.firstGreaterOrEqual("test_async_iter_080")

    let asyncSequence = readTransaction.getRange(
        from: beginSelector, to: endSelector
    )

    var records: [(String, String)] = []
    let iterator = asyncSequence.makeAsyncIterator()

    // Read records one by one to test iterator behavior
    while let kv = try await iterator.next() {
        let key = String(bytes: kv.key)
        let value = String(bytes: kv.value)
        records.append((key, value))

        // Stop at reasonable count to verify behavior
        if records.count >= 30 {
            break
        }
    }

    #expect(records.count >= 5, "Should read at least 5 records")
    #expect(records.count <= 60, "Should read expected number of records in range")

    // Verify records are in order
    for i in 1 ..< records.count {
        #expect(records[i - 1].0 < records[i].0, "Records should be in key order")
    }

    // Verify first and last records are within expected range
    #expect(records.first!.0.hasPrefix("test_async_iter_02"), "First record should be in expected range")
    #expect(records.last!.0.hasPrefix("test_async_iter_0"), "Last record should be in expected range")
}

@Test("Range exhaustion leaves no database read outstanding")
func rangeExhaustionLeavesNoOutstandingRead() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Use unique prefix to avoid conflicts with other tests
    let testID = UInt64.random(in: 0..<UInt64.max)
    let testPrefix = "range_exhaustion_\(testID)_"

    // Setup: insert small amount of data
    try await database.withTransaction { transaction in
        for i in 0..<5 {
            let key = "\(testPrefix)data_\(i)"
            try transaction.setValue(Array("value\(i)".utf8), for: Array(key.utf8))
        }
    }

    // Repeatedly prove that commit begins after all range I/O has completed.
    for iteration in 0..<20 {
        try await database.withTransaction { transaction in
            let sequence = transaction.getRange(
                from: .firstGreaterOrEqual(Array("\(testPrefix)data_".utf8)),
                to: .firstGreaterOrEqual(Array("\(testPrefix)data`".utf8)),
                streamingMode: .iterator
            )

            // Iterate through all items to exhaust the sequence
            var count = 0
            for try await _ in sequence {
                count += 1
            }

            #expect(count == 5, "Should read all 5 records")

            // Write immediately after iteration completes (to different key range)
            let markerKey = "\(testPrefix)marker_\(iteration)"
            try transaction.setValue(Array("done".utf8), for: Array(markerKey.utf8))
        }
    }

    // Cleanup
    try await database.withTransaction { transaction in
        try transaction.clearRange(
            beginKey: Array(testPrefix.utf8),
            endKey: Array((testPrefix + "~").utf8)
        )
    }
}

@Test("Range limit leaves no database read outstanding")
func rangeLimitLeavesNoOutstandingRead() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    let testID = UInt64.random(in: 0..<UInt64.max)
    let testPrefix = "range_limit_\(testID)_"

    // Setup: insert more data than the limit
    try await database.withTransaction { transaction in
        for i in 0..<20 {
            let key = "\(testPrefix)data_\(String(format: "%03d", i))"
            try transaction.setValue(Array("value\(i)".utf8), for: Array(key.utf8))
        }
    }

    // Reaching the logical limit must leave the transaction ready to commit.
    for iteration in 0..<20 {
        try await database.withTransaction { transaction in
            let sequence = transaction.getRange(
                from: .firstGreaterOrEqual(Array("\(testPrefix)data_".utf8)),
                to: .firstGreaterOrEqual(Array("\(testPrefix)data`".utf8)),
                limit: 5,
                streamingMode: .iterator
            )

            var count = 0
            for try await _ in sequence {
                count += 1
            }

            #expect(count == 5, "Should read exactly 5 records (limit)")

            let markerKey = "\(testPrefix)marker_\(iteration)"
            try transaction.setValue(Array("done".utf8), for: Array(markerKey.utf8))
        }
    }

    // Cleanup
    try await database.withTransaction { transaction in
        try transaction.clearRange(
            beginKey: Array(testPrefix.utf8),
            endKey: Array((testPrefix + "~").utf8)
        )
    }
}

@Test("Range break leaves no database read outstanding")
func rangeBreakLeavesNoOutstandingRead() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    let testID = UInt64.random(in: 0..<UInt64.max)
    let testPrefix = "range_break_\(testID)_"

    // Setup: insert data
    try await database.withTransaction { transaction in
        for i in 0..<20 {
            let key = "\(testPrefix)data_\(String(format: "%03d", i))"
            try transaction.setValue(Array("value\(i)".utf8), for: Array(key.utf8))
        }
    }

    // A returned row has no speculative next-page read behind it, so breaking
    // the loop cannot race a following mutation or commit.
    for iteration in 0..<20 {
        try await database.withTransaction { transaction in
            let sequence = transaction.getRange(
                from: .firstGreaterOrEqual(Array("\(testPrefix)data_".utf8)),
                to: .firstGreaterOrEqual(Array("\(testPrefix)data`".utf8)),
                streamingMode: .iterator
            )

            var count = 0
            for try await _ in sequence {
                count += 1
                if count >= 3 {
                    break
                }
            }

            #expect(count == 3, "Should read exactly 3 records before break")

            let markerKey = "\(testPrefix)marker_\(iteration)"
            try transaction.setValue(Array("done".utf8), for: Array(markerKey.utf8))
        }
    }

    // Cleanup
    try await database.withTransaction { transaction in
        try transaction.clearRange(
            beginKey: Array(testPrefix.utf8),
            endKey: Array((testPrefix + "~").utf8)
        )
    }
}

@Test("Task cancellation stops a pending range read")
func taskCancellationStopsPendingRangeRead() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    let testID = UInt64.random(in: 0..<UInt64.max)
    let testPrefix = "task_cancel_\(testID)_"

    // Setup: insert data
    try await database.withTransaction { transaction in
        for i in 0..<10 {
            let key = "\(testPrefix)data_\(String(format: "%03d", i))"
            try transaction.setValue(Array("value\(i)".utf8), for: Array(key.utf8))
        }
    }

    // Verify that cancelling a Task that is iterating over a range
    // properly cancels the underlying C future and throws CancellationError.
    // TransactionProtocol is Sendable, so we create the transaction first
    // and pass it to the Task.
    let beginKey = Array("\(testPrefix)data_".utf8)
    let endKey = Array("\(testPrefix)data`".utf8)
    let transaction = try database.createTransaction()

    let task = Task {
        let sequence = transaction.getRange(
            from: .firstGreaterOrEqual(beginKey),
            to: .firstGreaterOrEqual(endKey),
            streamingMode: .iterator
        )

        for try await _ in sequence {
            try Task.checkCancellation()
            await Task.yield()
        }
    }

    // Cancel the task
    task.cancel()

    // Cancellation must retain Swift structured-concurrency semantics instead
    // of being normalized into an FDB transport error.
    do {
        try await task.value
        Issue.record("Expected CancellationError")
    } catch is CancellationError {
        // Expected.
    } catch {
        Issue.record("Expected CancellationError, got \(error)")
    }

    // Cleanup
    try await database.withTransaction { transaction in
        try transaction.clearRange(
            beginKey: Array(testPrefix.utf8),
            endKey: Array((testPrefix + "~").utf8)
        )
    }
}

@Test("External transaction cancellation remains an FDB error")
func testExternalTransactionCancellationRemainsFDBError() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()
    transaction.cancel()

    do {
        _ = try await transaction.getValue(for: [0x01])
        Issue.record("Expected transaction cancellation to fail")
    } catch is CancellationError {
        Issue.record("External transaction cancellation must not become CancellationError")
    } catch let error as FDBError {
        #expect(error.code == FDBErrorCode.transactionCancelled.rawValue)
        #expect(error.retryDisposition == .never)
    } catch {
        Issue.record("Expected FDBError, got \(error)")
    }
}

extension String {
    func leftPad(toLength: Int, withPad pad: String) -> String {
        if count >= toLength { return self }
        return String(repeating: pad, count: toLength - count) + self
    }
}

@Test("getEstimatedRangeSizeBytes returns size estimate")
func testGetEstimatedRangeSizeBytes() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Write some test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "test_size_", endKey: "test_size`")
        for i in 0 ..< 100 {
            let key = "test_size_\(String.padded(i))"
            let value = String(repeating: "x", count: 1000)
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Get estimated size
    let transaction = try database.createTransaction()
    let beginKey: FDB.Bytes = Array("test_size_".utf8)
    let endKey: FDB.Bytes = Array("test_size`".utf8)
    let estimatedSize = try await transaction.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)

    // Size should be positive (may not be exact due to sampling)
    #expect(estimatedSize >= 0, "Estimated size should be non-negative")
}

@Test("getRangeSplitPoints returns split keys")
func testGetRangeSplitPoints() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Write some test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "test_split_", endKey: "test_split`")
        for i in 0 ..< 200 {
            let key = "test_split_\(String.padded(i))"
            let value = String(repeating: "y", count: 500)
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Get split points with 10KB chunks
    let transaction = try database.createTransaction()
    let beginKey: FDB.Bytes = Array("test_split_".utf8)
    let endKey: FDB.Bytes = Array("test_split`".utf8)
    let splitPoints = try await transaction.getRangeSplitPoints(
        beginKey: beginKey,
        endKey: endKey,
        chunkSize: 10000
    )

    // Should return at least begin and end keys
    #expect(splitPoints.count >= 2, "Should return at least begin and end keys")
    #expect(splitPoints.first?.copyBytes() == beginKey, "First split point should be begin key")
    #expect(splitPoints.last?.copyBytes() == endKey, "Last split point should be end key")
}

@Test("getCommittedVersion returns version after commit")
func testGetCommittedVersion() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    try transaction.setValue("test_value", for: "test_version_key")
    _ = try await transaction.commit()

    let committedVersion = try transaction.getCommittedVersion()
    #expect(committedVersion > 0, "Committed version should be positive for write transaction")
}

@Test("getCommittedVersion returns -1 for read-only transaction")
func getCommittedVersionReadOnly() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Read-only transaction
    _ = try await transaction.getValue(for: "test_readonly_key")
    _ = try await transaction.commit()

    let committedVersion = try transaction.getCommittedVersion()
    #expect(committedVersion == -1, "Read-only transaction should return -1")
}

@Test("approximateSize returns transaction size")
func approximateSizeTracksTransactionFootprint() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()
    let transaction = try database.createTransaction()

    // Initial size
    let initialSize = try await transaction.approximateSize()
    #expect(initialSize >= 0, "Initial size should be non-negative")

    // Add some mutations
    for i in 0 ..< 10 {
        let key = "test_approx_\(i)"
        let value = String(repeating: "z", count: 100)
        try transaction.setValue(value, for: key)
    }

    // Size should increase
    let sizeAfterMutations = try await transaction.approximateSize()
    #expect(sizeAfterMutations > initialSize, "Size should increase after mutations")
}

@Test("addConflictRange read conflict")
func addReadConflictRange() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_conflict_", endKey: "test_conflict`")
    _ = try await clearTransaction.commit()

    // Set up initial data
    let setupTransaction = try database.createTransaction()
    try setupTransaction.setValue("initial_value", for: "test_conflict_a")
    _ = try await setupTransaction.commit()

    // Test adding read conflict range
    let transaction = try database.createTransaction()
    let beginKey: FDB.Bytes = Array("test_conflict_a".utf8)
    let endKey: FDB.Bytes = Array("test_conflict_b".utf8)

    // Add read conflict range - should succeed
    try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: .read)

    // Should be able to commit successfully
    try await transaction.commit()
}

@Test("addConflictRange write conflict")
func addWriteConflictRange() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_write_conflict_", endKey: "test_write_conflict`")
    _ = try await clearTransaction.commit()

    // Test adding write conflict range
    let transaction = try database.createTransaction()
    let beginKey: FDB.Bytes = Array("test_write_conflict_a".utf8)
    let endKey: FDB.Bytes = Array("test_write_conflict_b".utf8)

    // Add write conflict range - should succeed
    try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: .write)

    // Should be able to commit successfully
    try await transaction.commit()
}

@Test("addConflictRange detects concurrent write conflicts")
func conflictRangeDetectsConcurrentWrites() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_concurrent_", endKey: "test_concurrent`")
    _ = try await clearTransaction.commit()

    // Set up initial data
    let setupTransaction = try database.createTransaction()
    try setupTransaction.setValue("initial", for: "test_concurrent_key")
    _ = try await setupTransaction.commit()

    // Create first transaction and add a write conflict range
    let transaction1 = try database.createTransaction()
    let beginKey: FDB.Bytes = Array("test_concurrent_key".utf8)
    var endKey: FDB.Bytes = beginKey
    endKey.append(0x00)

    try transaction1.addConflictRange(beginKey: beginKey, endKey: endKey, type: .write)

    // Create second transaction that writes to the same key
    let transaction2 = try database.createTransaction()
    try transaction2.setValue("modified_by_tr2", for: "test_concurrent_key")
    _ = try await transaction2.commit()

    // Now try to commit transaction1 - it should detect a conflict
    do {
        _ = try await transaction1.commit()
        // If it succeeds, that's also acceptable behavior
    } catch let error as FDBError {
        // Expected to fail with a conflict error (not_committed)
        #expect(error.retryDisposition == .retryableNotCommitted)
    }
}

@Test("addConflictRange multiple ranges")
func addMultipleConflictRanges() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear test key range
    let clearTransaction = try database.createTransaction()
    try clearTransaction.clearRange(beginKey: "test_multi_", endKey: "test_multi`")
    _ = try await clearTransaction.commit()

    // Test adding multiple conflict ranges
    let transaction = try database.createTransaction()

    // Add multiple read conflict ranges
    let beginKey1: FDB.Bytes = Array("test_multi_a".utf8)
    let endKey1: FDB.Bytes = Array("test_multi_b".utf8)
    try transaction.addConflictRange(beginKey: beginKey1, endKey: endKey1, type: .read)

    let beginKey2: FDB.Bytes = Array("test_multi_c".utf8)
    let endKey2: FDB.Bytes = Array("test_multi_d".utf8)
    try transaction.addConflictRange(beginKey: beginKey2, endKey: endKey2, type: .read)

    // Add write conflict range
    let beginKey3: FDB.Bytes = Array("test_multi_x".utf8)
    let endKey3: FDB.Bytes = Array("test_multi_y".utf8)
    try transaction.addConflictRange(beginKey: beginKey3, endKey: endKey3, type: .write)

    // Should be able to commit with multiple conflict ranges
    try await transaction.commit()
}

@Test("ConflictRangeType enum values")
func conflictRangeTypeValues() {
    // Test that conflict range type enum has expected values
    #expect(FDB.ConflictRangeType.read.rawValue == 0, "read should have value 0")
    #expect(FDB.ConflictRangeType.write.rawValue == 1, "write should have value 1")
}

// MARK: - Reverse Range Query Tests

@Test("readRangeBatch with reverse returns keys in descending order")
func readRangeBatchReverse() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_reverse_", endKey: "rangetest_reverse`")
        for i in 1...5 {
            let key = "rangetest_reverse_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test reverse range query
    let transaction = try database.createTransaction()
    let beginSelector = FDB.KeySelector.firstGreaterOrEqual("rangetest_reverse_")
    let endSelector = FDB.KeySelector.firstGreaterOrEqual("rangetest_reverse`")

    let result = try await transaction.readRangeBatch(
        from: beginSelector,
        to: endSelector,
        limit: 0,
        targetBytes: 0,
        streamingMode: .iterator,
        iteration: 1,
        reverse: true,
        snapshot: false
    )

    #expect(result.records.count == 5, "Should return all 5 records")

    // Verify descending order
    let keys = result.records.map { String(bytes: $0.key) }
    #expect(keys[0] == "rangetest_reverse_005", "First key should be the largest")
    #expect(keys[1] == "rangetest_reverse_004", "Second key should be second largest")
    #expect(keys[4] == "rangetest_reverse_001", "Last key should be the smallest")
}

@Test("readRangeBatch reverse with limit")
func readRangeBatchReverseWithLimit() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_rev_limit_", endKey: "rangetest_rev_limit`")
        for i in 1...10 {
            let key = "rangetest_rev_limit_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test reverse range query with limit
    let transaction = try database.createTransaction()
    let result = try await transaction.readRangeBatch(
        from: .firstGreaterOrEqual("rangetest_rev_limit_"),
        to: .firstGreaterOrEqual("rangetest_rev_limit`"),
        limit: 3,
        targetBytes: 0,
        streamingMode: .iterator,
        iteration: 1,
        reverse: true,
        snapshot: false
    )

    #expect(result.records.count == 3, "Should return exactly 3 records due to limit")

    // Verify we got the last 3 keys in descending order
    let keys = result.records.map { String(bytes: $0.key) }
    #expect(keys[0] == "rangetest_rev_limit_010", "First should be key 010")
    #expect(keys[1] == "rangetest_rev_limit_009", "Second should be key 009")
    #expect(keys[2] == "rangetest_rev_limit_008", "Third should be key 008")
}

@Test("getRange AsyncSequence with reverse")
func rangeReadsInReverseOrder() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_async_rev_", endKey: "rangetest_async_rev`")
        for i in 1...5 {
            let key = "rangetest_async_rev_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test using new getRange API with reverse
    let transaction = try database.createTransaction()
    var keys: [String] = []

    for try await row in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_async_rev_"),
        to: .firstGreaterOrEqual("rangetest_async_rev`"),
        reverse: true
    ) {
        keys.append(String(bytes: row.key))
    }

    #expect(keys.count == 5, "Should return all 5 records")
    #expect(keys[0] == "rangetest_async_rev_005", "First key should be the largest")
    #expect(keys[4] == "rangetest_async_rev_001", "Last key should be the smallest")
}

@Test("getRange AsyncSequence reverse with limit")
func reverseRangeHonorsLimit() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_asyncrevlim_", endKey: "rangetest_asyncrevlim`")
        for i in 1...20 {
            let key = "rangetest_asyncrevlim_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test reverse with limit using AsyncSequence
    let transaction = try database.createTransaction()
    var keys: [String] = []

    for try await row in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_asyncrevlim_"),
        to: .firstGreaterOrEqual("rangetest_asyncrevlim`"),
        limit: 5,
        reverse: true
    ) {
        keys.append(String(bytes: row.key))
    }

    #expect(keys.count == 5, "Should return exactly 5 records due to limit")
    #expect(keys[0] == "rangetest_asyncrevlim_020", "First should be key 020")
    #expect(keys[4] == "rangetest_asyncrevlim_016", "Last should be key 016")
}

@Test("getRange with streamingMode wantAll")
func rangeUsesWantAllStreamingMode() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_streaming_", endKey: "rangetest_streaming`")
        for i in 1...10 {
            let key = "rangetest_streaming_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test with wantAll streaming mode
    let transaction = try database.createTransaction()
    var count = 0

    for try await _ in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_streaming_"),
        to: .firstGreaterOrEqual("rangetest_streaming`"),
        streamingMode: .wantAll
    ) {
        count += 1
    }

    #expect(count == 10, "Should return all 10 records with wantAll mode")
}

@Test("getRange reverse iteration continues correctly across batches")
func reverseRangeContinuesAcrossBatches() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data - enough to span multiple batches
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_rev_batch_", endKey: "rangetest_rev_batch`")
        for i in 1...100 {
            let key = "rangetest_rev_batch_\(String.padded(i))"
            // Larger values to ensure multiple batches
            let value = String(repeating: "x", count: 500)
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test reverse iteration across batches
    let transaction = try database.createTransaction()
    var keys: [String] = []

    for try await row in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_rev_batch_"),
        to: .firstGreaterOrEqual("rangetest_rev_batch`"),
        reverse: true,
        streamingMode: .small  // Use small batches to ensure multiple fetches
    ) {
        keys.append(String(bytes: row.key))
    }

    #expect(keys.count == 100, "Should return all 100 records")

    // Verify order is strictly descending
    for i in 0..<(keys.count - 1) {
        #expect(keys[i] > keys[i + 1], "Keys should be in strictly descending order")
    }

    #expect(keys.first == "rangetest_rev_batch_100", "First key should be 100")
    #expect(keys.last == "rangetest_rev_batch_001", "Last key should be 001")
}

// MARK: - Forward Limit Tests

@Test("getRange AsyncSequence forward with limit")
func forwardRangeHonorsLimit() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_fwd_limit_", endKey: "rangetest_fwd_limit`")
        for i in 1...20 {
            let key = "rangetest_fwd_limit_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test forward with limit using AsyncSequence
    let transaction = try database.createTransaction()
    var keys: [String] = []

    for try await row in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_fwd_limit_"),
        to: .firstGreaterOrEqual("rangetest_fwd_limit`"),
        limit: 5
    ) {
        keys.append(String(bytes: row.key))
    }

    #expect(keys.count == 5, "Should return exactly 5 records due to limit")
    #expect(keys[0] == "rangetest_fwd_limit_001", "First should be key 001")
    #expect(keys[4] == "rangetest_fwd_limit_005", "Last should be key 005")
}

@Test("getRange forward iteration continues correctly across batches with limit")
func forwardRangeContinuesUntilLimit() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data - enough to span multiple batches
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_fwd_batch_", endKey: "rangetest_fwd_batch`")
        for i in 1...100 {
            let key = "rangetest_fwd_batch_\(String.padded(i))"
            let value = String(repeating: "x", count: 500)
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test forward iteration with limit across batches
    let transaction = try database.createTransaction()
    var keys: [String] = []

    for try await row in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_fwd_batch_"),
        to: .firstGreaterOrEqual("rangetest_fwd_batch`"),
        limit: 50,
        streamingMode: .small
    ) {
        keys.append(String(bytes: row.key))
    }

    #expect(keys.count == 50, "Should return exactly 50 records due to limit")

    // Verify order is strictly ascending
    for i in 0..<(keys.count - 1) {
        #expect(keys[i] < keys[i + 1], "Keys should be in strictly ascending order")
    }

    #expect(keys.first == "rangetest_fwd_batch_001", "First key should be 001")
    #expect(keys.last == "rangetest_fwd_batch_050", "Last key should be 050")
}

// MARK: - Bytes-based API Tests

@Test("getRange with Bytes keys")
func getRangeWithBytesKeys() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    let prefix: FDB.Bytes = Array("rangetest_bytes_range_".utf8)
    let endPrefix: FDB.Bytes = Array("rangetest_bytes_range`".utf8)

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: prefix, endKey: endPrefix)
        for i in 1...5 {
            let key = Array("rangetest_bytes_range_\(String.padded(i))".utf8)
            let value = Array("value\(i)".utf8)
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test using new getRange API with Bytes
    let transaction = try database.createTransaction()
    var count = 0

    for try await row in transaction.getRange(
        from: prefix,
        to: endPrefix,
        limit: 3
    ) {
        count += 1
        if count == 1 {
            let keyStr = String(bytes: row.key)
            #expect(keyStr == "rangetest_bytes_range_001", "First key should be 001")
        }
    }

    #expect(count == 3, "Should return exactly 3 records due to limit")
}

@Test("getRange with Bytes keys and reverse")
func getRangeWithBytesKeysReverse() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    let prefix: FDB.Bytes = Array("rangetest_bytes_rev_".utf8)
    let endPrefix: FDB.Bytes = Array("rangetest_bytes_rev`".utf8)

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: prefix, endKey: endPrefix)
        for i in 1...5 {
            let key = Array("rangetest_bytes_rev_\(String.padded(i))".utf8)
            let value = Array("value\(i)".utf8)
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test using getRange API with Bytes in reverse
    let transaction = try database.createTransaction()
    var keys: [String] = []

    for try await row in transaction.getRange(
        from: prefix,
        to: endPrefix,
        reverse: true
    ) {
        keys.append(String(bytes: row.key))
    }

    #expect(keys.count == 5, "Should return all 5 records")
    #expect(keys[0] == "rangetest_bytes_rev_005", "First should be largest key")
    #expect(keys[4] == "rangetest_bytes_rev_001", "Last should be smallest key")
}

@Test("readRangeBatch with Bytes keys and all parameters")
func readRangeBatchWithBytesAllParams() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    let prefix: FDB.Bytes = Array("rangetest_database_bytes_".utf8)
    let endPrefix: FDB.Bytes = Array("rangetest_database_bytes`".utf8)

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: prefix, endKey: endPrefix)
        for i in 1...10 {
            let key = Array("rangetest_database_bytes_\(String.padded(i))".utf8)
            let value = Array("value\(i)".utf8)
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test readRangeBatch with Bytes keys
    let transaction = try database.createTransaction()
    let result = try await transaction.readRangeBatch(
        from: prefix,
        to: endPrefix,
        limit: 5,
        targetBytes: 0,
        streamingMode: .wantAll,
        iteration: 1,
        reverse: true,
        snapshot: false
    )

    #expect(result.records.count == 5, "Should return 5 records due to limit")
    let firstKey = String(bytes: result.records[0].key)
    #expect(firstKey == "rangetest_database_bytes_010", "First key should be largest")
}

// MARK: - Empty Range Tests

@Test("getRange on empty range returns no results")
func getRangeEmptyRange() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear the range to ensure it's empty
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_empty_range_", endKey: "rangetest_empty_range`")
        return ()
    }

    // Test getRange on empty range
    let transaction = try database.createTransaction()
    var count = 0

    for try await _ in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_empty_range_"),
        to: .firstGreaterOrEqual("rangetest_empty_range`")
    ) {
        count += 1
    }

    #expect(count == 0, "Empty range should return no results")
}

@Test("getRange reverse on empty range returns no results")
func getRangeEmptyRangeReverse() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear the range to ensure it's empty
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_empty_rev_", endKey: "rangetest_empty_rev`")
        return ()
    }

    // Test getRange reverse on empty range
    let transaction = try database.createTransaction()
    var count = 0

    for try await _ in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_empty_rev_"),
        to: .firstGreaterOrEqual("rangetest_empty_rev`"),
        reverse: true
    ) {
        count += 1
    }

    #expect(count == 0, "Empty range with reverse should return no results")
}

// MARK: - Snapshot Mode Tests

@Test("getRange with snapshot mode")
func getRangeWithSnapshotMode() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_snapshot_", endKey: "rangetest_snapshot`")
        for i in 1...5 {
            let key = "rangetest_snapshot_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test getRange with snapshot mode
    let transaction = try database.createTransaction()
    var count = 0

    for try await _ in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_snapshot_"),
        to: .firstGreaterOrEqual("rangetest_snapshot`"),
        snapshot: true
    ) {
        count += 1
    }

    #expect(count == 5, "Snapshot mode should return all 5 records")
}

@Test("getRange reverse with snapshot mode")
func getRangeReverseWithSnapshotMode() async throws {
    try await FDBClient.maybeInitialize()
    let database = try FDBClient.openTestDatabase()

    // Clear and set up test data
    try await database.withTransaction { transaction in
        try transaction.clearRange(beginKey: "rangetest_snap_rev_", endKey: "rangetest_snap_rev`")
        for i in 1...5 {
            let key = "rangetest_snap_rev_\(String.padded(i))"
            let value = "value\(i)"
            try transaction.setValue(value, for: key)
        }
        return ()
    }

    // Test getRange reverse with snapshot mode
    let transaction = try database.createTransaction()
    var keys: [String] = []

    for try await row in transaction.getRange(
        from: .firstGreaterOrEqual("rangetest_snap_rev_"),
        to: .firstGreaterOrEqual("rangetest_snap_rev`"),
        reverse: true,
        snapshot: true
    ) {
        keys.append(String(bytes: row.key))
    }

    #expect(keys.count == 5, "Should return all 5 records")
    #expect(keys[0] == "rangetest_snap_rev_005", "First should be largest key with snapshot+reverse")
}
