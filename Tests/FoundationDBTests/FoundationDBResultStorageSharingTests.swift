import CFoundationDB
import DatabaseTypes
import Synchronization
import Testing

@testable import FoundationDB

@Suite("FoundationDB result storage sharing")
struct FoundationDBResultStorageSharingTests {
    @Test("Future value shares the FoundationDB result storage")
    func futureValueSharesFoundationDBResultStorage() async throws {
        try await FDBClient.maybeInitialize()
        let database = try openDatabase()
        defer { fdb_database_destroy(database) }

        let key = Array("shared-future-value".utf8)
        let expectedValue = Array("foundationdb-result-storage".utf8)
        let writeTransaction = try createTransaction(database)
        defer { fdb_transaction_destroy(writeTransaction) }
        writeValue(expectedValue, for: key, transaction: writeTransaction)
        _ = try await Future<CompletionResultDecoder>(
            fdb_transaction_commit(writeTransaction)
        ).value
        let readTransaction = try createTransaction(database)
        defer { fdb_transaction_destroy(readTransaction) }
        let readFuture = try #require(key.withUnsafeBytes { keyBytes in
            fdb_transaction_get(
                readTransaction,
                keyBytes.bindMemory(to: UInt8.self).baseAddress,
                Int32(key.count),
                1
            )
        })
        var future: Future<ValueResultDecoder>? = Future<ValueResultDecoder>(readFuture)
        var value: ByteString? = try await future?.value
        let retainedValue = try #require(value)

        future = nil
        value = nil

        var present: Int32 = 0
        var sourceBytes: UnsafePointer<UInt8>?
        var resultLength: Int32 = 0
        let error = fdb_future_get_value(
            readFuture,
            &present,
            &sourceBytes,
            &resultLength
        )
        #expect(error == 0)
        #expect(present != 0)
        #expect(resultLength == Int32(expectedValue.count))
        let sourceStorageIdentity = try #require(
            sourceBytes.map(UInt.init(bitPattern:))
        )
        let retainedStorageIdentity = retainedValue.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }

        #expect(retainedStorageIdentity == sourceStorageIdentity)
        #expect(retainedValue == ByteString(expectedValue))
    }

    @Test("Range records retain the FoundationDB batch storage")
    func rangeRecordsRetainResultBatchStorage() async throws {
        try await FDBClient.maybeInitialize()
        let database = try openDatabase()
        defer { fdb_database_destroy(database) }

        let prefix = Array("shared-range-".utf8)
        let firstKey = prefix + [0x01]
        let secondKey = prefix + [0x02]
        let firstValue = Array("first-shared-range-value".utf8)
        let secondValue = Array("second-shared-range-value".utf8)
        let writeTransaction = try createTransaction(database)
        defer { fdb_transaction_destroy(writeTransaction) }
        writeValue(firstValue, for: firstKey, transaction: writeTransaction)
        writeValue(secondValue, for: secondKey, transaction: writeTransaction)
        _ = try await Future<CompletionResultDecoder>(
            fdb_transaction_commit(writeTransaction)
        ).value
        let readTransaction = try createTransaction(database)
        defer { fdb_transaction_destroy(readTransaction) }
        let rangeEnd = prefix + [0xFF]
        let rangeFuture = try #require(prefix.withUnsafeBytes { beginBytes in
            rangeEnd.withUnsafeBytes { endBytes in
                fdb_transaction_get_range(
                    readTransaction,
                    beginBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(prefix.count),
                    0,
                    1,
                    endBytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(rangeEnd.count),
                    0,
                    1,
                    0,
                    0,
                    FDBStreamingMode(FDB.StreamingMode.wantAll.rawValue),
                    1,
                    1,
                    0
                )
            }
        })
        var future: Future<RangeBatchResultDecoder>? = Future<RangeBatchResultDecoder>(rangeFuture)
        var batch: RangeBatch? = try await future?.value
        #expect(batch?.records.count == 2)
        var decodedRecords: [FDB.KeyValue]? = batch?.records

        var sourceRecords: UnsafePointer<FDBKeyValue>?
        var sourceRecordCount: Int32 = 0
        var hasMore: Int32 = 0
        let error = fdb_future_get_keyvalue_array(
            rangeFuture,
            &sourceRecords,
            &sourceRecordCount,
            &hasMore
        )
        #expect(error == 0)
        #expect(sourceRecordCount == 2)
        #expect(hasMore == 0)
        let sourceRecordView = UnsafeBufferPointer(
            start: try #require(sourceRecords),
            count: Int(sourceRecordCount)
        )
        let expectedRows = [
            (firstKey, firstValue),
            (secondKey, secondValue)
        ]
        for index in sourceRecordView.indices {
            let sourceRecord = sourceRecordView[index]
            let decodedRecord = try #require(decodedRecords)[index]
            let sourceKeyStorageIdentity = try #require(
                sourceRecord.key.map(UInt.init(bitPattern:))
            )
            let sourceValueStorageIdentity = try #require(
                sourceRecord.value.map(UInt.init(bitPattern:))
            )
            let decodedKeyStorageIdentity = decodedRecord.key.withUnsafeBytes {
                $0.baseAddress.map(UInt.init(bitPattern:))
            }
            let decodedValueStorageIdentity = decodedRecord.value.withUnsafeBytes {
                $0.baseAddress.map(UInt.init(bitPattern:))
            }

            #expect(Int(sourceRecord.key_length) == decodedRecord.key.count)
            #expect(Int(sourceRecord.value_length) == decodedRecord.value.count)
            #expect(decodedKeyStorageIdentity == sourceKeyStorageIdentity)
            #expect(decodedValueStorageIdentity == sourceValueStorageIdentity)
            #expect(decodedRecord.key == ByteString(expectedRows[index].0))
            #expect(decodedRecord.value == ByteString(expectedRows[index].1))
        }

        var retainedRecord = try #require(decodedRecords?.first)
        decodedRecords = nil
        future = nil
        batch = nil

        sourceRecords = nil
        sourceRecordCount = 0
        hasMore = 0
        let retainedError = fdb_future_get_keyvalue_array(
            rangeFuture,
            &sourceRecords,
            &sourceRecordCount,
            &hasMore
        )
        #expect(retainedError == 0)
        #expect(sourceRecordCount == 2)
        let firstSourceRecord = try #require(sourceRecords).pointee
        let sourceKeyStorageIdentity = try #require(
            firstSourceRecord.key.map(UInt.init(bitPattern:))
        )
        let sourceValueStorageIdentity = try #require(
            firstSourceRecord.value.map(UInt.init(bitPattern:))
        )
        let retainedKeyStorageIdentity = retainedRecord.key.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }
        let retainedValueStorageIdentity = retainedRecord.value.withUnsafeBytes {
            $0.baseAddress.map(UInt.init(bitPattern:))
        }

        #expect(retainedKeyStorageIdentity == sourceKeyStorageIdentity)
        #expect(retainedValueStorageIdentity == sourceValueStorageIdentity)
        #expect(retainedRecord.key == ByteString(firstKey))
        #expect(retainedRecord.value == ByteString(firstValue))

        retainedRecord = FDB.KeyValue(key: [], value: [])
    }

    @Test("Input sources are borrowed once and copied by the C call")
    func inputSourcesAreBorrowedOnceAtCallTime() async throws {
        try await FDBClient.maybeInitialize()
        let database = try FDBClient.openTestDatabase()
        let originalKey = Array("borrowed-call-time-key".utf8)
        let originalValue = Array("call-time-value".utf8)
        let key = MutableTrackingByteInput(bytes: originalKey)
        let value = MutableTrackingByteInput(bytes: originalValue)
        let transaction = try database.createTransaction()

        try transaction.setValue(value, for: key)
        #expect(key.borrowCount == 1)
        #expect(value.borrowCount == 1)

        key.replace(with: Array("mutated-call-time-key".utf8))
        value.replace(with: Array("mutated-value".utf8))
        _ = try await transaction.commit()

        let readTransaction = try database.createTransaction()
        let storedValue = try #require(
            await readTransaction.getValue(for: originalKey, snapshot: true)
        )
        #expect(storedValue == ByteString(originalValue))
        let mutatedKeyValue = try await readTransaction.getValue(
            for: key.currentBytes,
            snapshot: true
        )
        #expect(mutatedKeyValue == nil)
    }

    @Test("Present empty values remain distinct from missing values")
    func emptyValueRemainsPresent() async throws {
        try await FDBClient.maybeInitialize()
        let database = try FDBClient.openTestDatabase()
        let key = Array("present-empty-value".utf8)
        let transaction = try database.createTransaction()

        try transaction.setValue([UInt8](), for: key)
        _ = try await transaction.commit()

        let readTransaction = try database.createTransaction()
        let value = try await readTransaction.getValue(for: key, snapshot: true)
        let presentValue = try #require(value)
        #expect(presentValue.isEmpty)
    }

    @Test("Range integer inputs fail before reaching FoundationDB")
    func rangeIntegerInputsAreValidated() async throws {
        try await FDBClient.maybeInitialize()
        let database = try FDBClient.openTestDatabase()
        let transaction = try database.createTransaction()
        let key = Array("range-input-validation".utf8)
        let selector = FDB.KeySelector.firstGreaterOrEqual(key)
        let oversized = Int(Int32.max) + 1
        let oversizedInput = OversizedByteInput(byteCount: oversized)

        #expect(throws: FDB.InputError.byteCountOutOfRange(oversized)) {
            try transaction.clear(key: oversizedInput)
        }
        #expect(oversizedInput.borrowCount == 1)

        await #expect(throws: FDB.InputError.integerOutOfRange(
            parameter: "limit",
            value: oversized
        )) {
            try await transaction.readRangeBatch(
                from: selector,
                to: selector,
                limit: oversized,
                targetBytes: 0,
                streamingMode: .iterator,
                iteration: 1,
                reverse: false,
                snapshot: true
            )
        }

        await #expect(throws: FDB.InputError.integerOutOfRange(
            parameter: "targetBytes",
            value: oversized
        )) {
            try await transaction.readRangeBatch(
                from: selector,
                to: selector,
                limit: 0,
                targetBytes: oversized,
                streamingMode: .iterator,
                iteration: 1,
                reverse: false,
                snapshot: true
            )
        }

        await #expect(throws: FDB.InputError.integerOutOfRange(
            parameter: "iteration",
            value: oversized
        )) {
            try await transaction.readRangeBatch(
                from: selector,
                to: selector,
                limit: 0,
                targetBytes: 0,
                streamingMode: .iterator,
                iteration: oversized,
                reverse: false,
                snapshot: true
            )
        }

        let oversizedSelector = FDB.KeySelector(
            key: key,
            orEqual: false,
            offset: oversized
        )
        await #expect(throws: FDB.InputError.integerOutOfRange(
            parameter: "begin.offset",
            value: oversized
        )) {
            try await transaction.readRangeBatch(
                from: oversizedSelector,
                to: selector,
                limit: 0,
                targetBytes: 0,
                streamingMode: .iterator,
                iteration: 1,
                reverse: false,
                snapshot: true
            )
        }
    }
}

private final class OversizedByteInput: FDB.ByteInput, Sendable {
    let byteCount: Int
    private let state = Mutex(0)

    init(byteCount: Int) {
        self.byteCount = byteCount
    }

    var borrowCount: Int { state.withLock { $0 } }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        state.withLock { $0 += 1 }
        return try body(UnsafeRawBufferPointer(
            start: UnsafeRawPointer(bitPattern: 1),
            count: byteCount
        ))
    }
}

private final class MutableTrackingByteInput: FDB.ByteInput, Sendable {
    private struct State: Sendable {
        var bytes: [UInt8]
        var borrowCount = 0
    }

    private let state: Mutex<State>

    init(bytes: [UInt8]) {
        self.state = Mutex(State(bytes: bytes))
    }

    var borrowCount: Int { state.withLock { $0.borrowCount } }
    var currentBytes: [UInt8] { state.withLock { $0.bytes } }

    func replace(with bytes: [UInt8]) {
        state.withLock { $0.bytes = bytes }
    }

    func withUnsafeBytes<Result>(
        _ body: (UnsafeRawBufferPointer) throws -> Result
    ) rethrows -> Result {
        let bytes = state.withLock { state in
            state.borrowCount += 1
            return state.bytes
        }
        return try bytes.withUnsafeBytes(body)
    }
}

private func openDatabase() throws -> OpaquePointer {
    var database: OpaquePointer?
    let error = fdb_create_database(FDBClient.testClusterFilePath, &database)
    guard error == 0 else { throw FDBError(code: error) }
    return try #require(database)
}

private func createTransaction(
    _ database: OpaquePointer
) throws -> OpaquePointer {
    var transaction: OpaquePointer?
    let error = fdb_database_create_transaction(database, &transaction)
    guard error == 0 else { throw FDBError(code: error) }
    return try #require(transaction)
}

private func writeValue(
    _ value: [UInt8],
    for key: [UInt8],
    transaction: OpaquePointer
) {
    key.withUnsafeBytes { keyBytes in
        value.withUnsafeBytes { valueBytes in
            fdb_transaction_set(
                transaction,
                keyBytes.bindMemory(to: UInt8.self).baseAddress,
                Int32(key.count),
                valueBytes.bindMemory(to: UInt8.self).baseAddress,
                Int32(value.count)
            )
        }
    }
}
