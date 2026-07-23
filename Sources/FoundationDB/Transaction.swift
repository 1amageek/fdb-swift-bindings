/*
 * Transaction.swift
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

public final class FDBTransaction: TransactionProtocol, Sendable {
    private let transactionAddress: UInt

    init(transaction: OpaquePointer) {
        self.transactionAddress = UInt(bitPattern: transaction)
    }

    deinit {
        fdb_transaction_destroy(transaction)
    }

    private var transaction: OpaquePointer {
        guard let transaction = OpaquePointer(bitPattern: transactionAddress) else {
            preconditionFailure("FoundationDB transaction pointer address is invalid")
        }
        return transaction
    }

    public func getValue<Key: FDB.ByteInput>(
        for key: Key,
        snapshot: Bool
    ) async throws -> FDB.ByteString? {
        let future = try withInputBytes(key) { keyBytes, keyLength in
            Future<ValueResultDecoder>(
                fdb_transaction_get(
                    transaction,
                    keyBytes,
                    keyLength,
                    snapshot ? 1 : 0
                )
            )
        }
        return try await future.value
    }

    public func setValue<Value: FDB.ByteInput, Key: FDB.ByteInput>(
        _ value: Value,
        for key: Key
    ) throws {
        try withInputBytes(key) { keyBytes, keyLength in
            try withInputBytes(value) { valueBytes, valueLength in
                fdb_transaction_set(
                    transaction,
                    keyBytes,
                    keyLength,
                    valueBytes,
                    valueLength
                )
            }
        }
    }

    public func clear<Key: FDB.ByteInput>(key: Key) throws {
        try withInputBytes(key) { keyBytes, keyLength in
            fdb_transaction_clear(
                transaction,
                keyBytes,
                keyLength
            )
        }
    }

    public func clearRange<Begin: FDB.ByteInput, End: FDB.ByteInput>(
        beginKey: Begin,
        endKey: End
    ) throws {
        try withInputBytes(beginKey) { beginKeyBytes, beginKeyLength in
            try withInputBytes(endKey) { endKeyBytes, endKeyLength in
                fdb_transaction_clear_range(
                    transaction,
                    beginKeyBytes,
                    beginKeyLength,
                    endKeyBytes,
                    endKeyLength
                )
            }
        }
    }

    public func atomicOp<Key: FDB.ByteInput, Parameter: FDB.ByteInput>(
        key: Key,
        param: Parameter,
        mutationType: FDB.MutationType
    ) throws {
        try withInputBytes(key) { keyBytes, keyLength in
            try withInputBytes(param) { paramBytes, parameterLength in
                fdb_transaction_atomic_op(
                    transaction,
                    keyBytes,
                    keyLength,
                    paramBytes,
                    parameterLength,
                    FDBMutationType(mutationType.rawValue)
                )
            }
        }
    }

    public func setOption<Value: FDB.ByteInput>(
        to value: Value,
        forOption option: FDB.TransactionOption
    ) throws {
        let error = try withInputBytes(value) { bytes, length in
            fdb_transaction_set_option(
                transaction,
                FDBTransactionOption(option.rawValue),
                bytes,
                length
            )
        }

        if error != 0 {
            throw FDBError(code: error)
        }
    }

    public func setOption(forOption option: FDB.TransactionOption) throws {
        let error = fdb_transaction_set_option(
            transaction,
            FDBTransactionOption(option.rawValue),
            nil,
            0
        )

        if error != 0 {
            throw FDBError(code: error)
        }
    }

    public func getKey(
        selector: FDB.KeySelector,
        snapshot: Bool
    ) async throws -> FDB.ByteString {
        let offset = try validatedParameter(
            selector.offset,
            named: "selector.offset"
        )
        let future = try withInputBytes(
            selector.key
        ) { keyBytes, keyLength in
            Future<KeyResultDecoder>(
                fdb_transaction_get_key(
                    transaction,
                    keyBytes,
                    keyLength,
                    selector.orEqual ? 1 : 0,
                    offset,
                    snapshot ? 1 : 0
                )
            )
        }
        return try await future.value
    }

    public func commit() async throws {
        _ = try await Future<CompletionResultDecoder>(
            fdb_transaction_commit(transaction)
        ).value
    }

    public func cancel() {
        fdb_transaction_cancel(transaction)
    }

    public func requestVersionstamp() -> any FDB.PendingTransactionVersionstamp {
        TransactionVersionstampRequest(
            future: Future<TransactionVersionstampResultDecoder>(
                fdb_transaction_get_versionstamp(transaction)
            )
        )
    }

    public func setReadVersion(_ version: FDB.Version) {
        fdb_transaction_set_read_version(transaction, version)
    }

    public func getReadVersion() async throws -> FDB.Version {
        try await Future<VersionResultDecoder>(
            fdb_transaction_get_read_version(transaction)
        ).value
    }

    public func onError(_ error: FDBError) async throws {
        _ = try await Future<CompletionResultDecoder>(
            fdb_transaction_on_error(transaction, error.code)
        ).value
    }

    public func getEstimatedRangeSizeBytes<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(beginKey: Begin, endKey: End) async throws -> Int64 {
        let future = try withInputBytes(
            beginKey
        ) { beginKeyBytes, beginKeyLength in
            try withInputBytes(endKey) { endKeyBytes, endKeyLength in
                Future<Int64ResultDecoder>(
                    fdb_transaction_get_estimated_range_size_bytes(
                        transaction,
                        beginKeyBytes,
                        beginKeyLength,
                        endKeyBytes,
                        endKeyLength
                    )
                )
            }
        }
        return try await future.value
    }

    public func getRangeSplitPoints<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        chunkSize: Int64
    ) async throws -> [FDB.ByteString] {
        let future = try withInputBytes(
            beginKey
        ) { beginKeyBytes, beginKeyLength in
            try withInputBytes(endKey) { endKeyBytes, endKeyLength in
                Future<KeyCollectionResultDecoder>(
                    fdb_transaction_get_range_split_points(
                        transaction,
                        beginKeyBytes,
                        beginKeyLength,
                        endKeyBytes,
                        endKeyLength,
                        chunkSize
                    )
                )
            }
        }
        return try await future.value
    }

    public func getCommittedVersion() throws -> FDB.Version {
        var version: FDB.Version = 0
        let err = fdb_transaction_get_committed_version(transaction, &version)
        if err != 0 {
            throw FDBError(code: err)
        }
        return version
    }

    public func approximateSize() async throws -> Int64 {
        try await Future<Int64ResultDecoder>(
            fdb_transaction_get_approximate_size(transaction)
        ).value
    }

    public func addConflictRange<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        type: FDB.ConflictRangeType
    ) throws {
        let error = try withInputBytes(
            beginKey
        ) { beginKeyBytes, beginKeyLength in
            try withInputBytes(endKey) { endKeyBytes, endKeyLength in
                fdb_transaction_add_conflict_range(
                    transaction,
                    beginKeyBytes,
                    beginKeyLength,
                    endKeyBytes,
                    endKeyLength,
                    FDBConflictRangeType(rawValue: type.rawValue)
                )
            }
        }

        if error != 0 {
            throw FDBError(code: error)
        }
    }

    public func readRangeBatch(
        from begin: FDB.KeySelector,
        to end: FDB.KeySelector,
        limit: Int,
        targetBytes: Int,
        streamingMode: FDB.StreamingMode,
        iteration: Int,
        reverse: Bool,
        snapshot: Bool
    ) async throws -> RangeBatch {
        let beginOffset = try validatedParameter(
            begin.offset,
            named: "begin.offset"
        )
        let endOffset = try validatedParameter(
            end.offset,
            named: "end.offset"
        )
        let rowLimit = try validatedParameter(limit, named: "limit")
        let byteTarget = try validatedParameter(
            targetBytes,
            named: "targetBytes"
        )
        let iteration = try validatedParameter(
            iteration,
            named: "iteration"
        )
        let future = try withInputBytes(
            begin.key
        ) { beginKeyBytes, beginKeyLength in
            try withInputBytes(end.key) { endKeyBytes, endKeyLength in
                Future<RangeBatchResultDecoder>(
                    fdb_transaction_get_range(
                        transaction,
                        beginKeyBytes,
                        beginKeyLength,
                        begin.orEqual ? 1 : 0,
                        beginOffset,
                        endKeyBytes,
                        endKeyLength,
                        end.orEqual ? 1 : 0,
                        endOffset,
                        rowLimit,
                        byteTarget,
                        FDBStreamingMode(streamingMode.rawValue),
                        iteration,
                        snapshot ? 1 : 0,
                        reverse ? 1 : 0
                    )
                )
            }
        }

        return try await future.value
    }
}
