/*
 * Future.swift
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

/// Opaque handle for one FoundationDB future.
typealias FutureHandle = OpaquePointer

/// Decodes one result shape from a FoundationDB C future.
///
/// Decoder types associate a FoundationDB future result shape with its final
/// output. They mark result shapes and do not represent intermediate values.
protocol ResultDecoder: Sendable {
    associatedtype Output: Sendable

    /// Extracts the result value from a FoundationDB future.
    ///
    /// - Parameter future: The future handle to extract from.
    /// - Returns: The extracted result value.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws -> Output
}

/// An async owner for FoundationDB futures.
///
/// `Future<Decoder>` presents FoundationDB completion through structured
/// concurrency so operations can be awaited naturally.
///
/// ## Usage Example
/// ```swift
/// let future = Future<ValueResultDecoder>(futureHandle)
/// let result = try await future.value
/// ```
final class Future<Decoder: ResultDecoder>: Sendable {
    /// The owner of the underlying FoundationDB future.
    private let owner: FutureOwner

    /// Initializes a future with the given FoundationDB handle.
    ///
    /// - Parameter futureHandle: The FoundationDB future to own.
    init(_ futureHandle: FutureHandle) {
        self.owner = FutureOwner(futureHandle)
    }

    /// Asynchronously waits for the future to complete and returns the result.
    ///
    /// This property allows the caller to await the underlying C future.
    /// Swift Task cancellation is propagated to the C layer via `fdb_future_cancel()`.
    ///
    /// - Returns: The result value extracted from the future.
    /// - Throws: `FDBError` if the future operation failed or was cancelled.
    var value: Decoder.Output {
        get async throws {
            let owner = self.owner
            if Task.isCancelled {
                owner.cancel()
                throw CancellationError()
            }
            return try await withTaskCancellationHandler {
                try await withCheckedThrowingContinuation {
                    (continuation: CheckedContinuation<Decoder.Output, Error>) in
                    let readinessRegistration = FutureReadinessRegistration {
                        [continuation, owner] future in
                        do {
                            guard let future else {
                                throw FDBError(.internalError)
                            }

                            let error = fdb_future_get_error(future)
                            if error != 0 {
                                if error == FDBErrorCode.operationCancelled.rawValue,
                                   owner.cancellationWasRequested {
                                    throw CancellationError()
                                }
                                throw FDBError(code: error)
                            }

                            let value = try withExtendedLifetime(owner) {
                                try Decoder.decode(
                                    from: future,
                                    retaining: owner
                                )
                            }
                            continuation.resume(returning: value)
                        } catch {
                            continuation.resume(throwing: error)
                        }
                    }

                    let registration = Unmanaged
                        .passRetained(readinessRegistration)
                        .toOpaque()
                    let registrationError = owner.registerReadyHandler(
                        deliverFutureReadiness,
                        registration: registration
                    )
                    if registrationError != 0 {
                        _ = Unmanaged<FutureReadinessRegistration>
                            .fromOpaque(registration)
                            .takeRetainedValue()
                        if registrationError == FDBErrorCode.operationCancelled.rawValue,
                           owner.cancellationWasRequested {
                            continuation.resume(throwing: CancellationError())
                        } else {
                            continuation.resume(
                                throwing: FDBError(code: registrationError)
                            )
                        }
                    }
                }
            } onCancel: {
                owner.cancel()
            }
        }
    }
}

/// Owns a FoundationDB C future across callback and cancellation races.
///
/// The C API documents future operations as thread-safe. Storing the pointer
/// address as a value gives Swift a Sendable owner while ARC guarantees that
/// destruction cannot occur until the operation, callback, and cancellation
/// handler have all released the owner.
final class FutureOwner: Sendable {
    private let futureIdentity: UInt
    private let cancellationRequested = Mutex(false)

    init(_ future: FutureHandle) {
        self.futureIdentity = UInt(bitPattern: future)
    }

    deinit {
        fdb_future_destroy(handle)
    }

    func registerReadyHandler(
        _ handler: @escaping FDBCallback,
        registration: UnsafeMutableRawPointer
    ) -> Int32 {
        fdb_future_set_callback(handle, handler, registration)
    }

    func cancel() {
        cancellationRequested.withLock { $0 = true }
        fdb_future_cancel(handle)
    }

    var cancellationWasRequested: Bool {
        cancellationRequested.withLock { $0 }
    }

    private var handle: FutureHandle {
        guard let handle = OpaquePointer(bitPattern: futureIdentity) else {
            preconditionFailure("FoundationDB future identity is invalid")
        }
        return handle
    }
}

/// Retains the handler registered for a FoundationDB future's ready event.
///
/// FoundationDB owns this registration until the future becomes ready.
private final class FutureReadinessRegistration: Sendable {
    /// Handles the transition to the ready state.
    let handler: @Sendable (FutureHandle?) -> Void

    /// Initializes a ready-state context with its handler.
    ///
    /// - Parameter handler: The handler to invoke when the future is ready.
    init(handler: @escaping @Sendable (FutureHandle?) -> Void) {
        self.handler = handler
    }
}

/// Handles the C future's transition to the ready state.
///
/// This function is called by the FoundationDB C API when a future completes.
/// It consumes the retained registration and invokes its ready handler.
///
/// - Parameters:
///   - future: The completed C future pointer.
///   - registration: Retained `FutureReadinessRegistration`.
@_cdecl("foundationdb_future_did_become_ready")
private func deliverFutureReadiness(
    _ future: FutureHandle?,
    _ registration: UnsafeMutableRawPointer?
) {
    guard let registration else { return }
    let readinessRegistration = Unmanaged<FutureReadinessRegistration>
        .fromOpaque(registration)
        .takeRetainedValue()
    readinessRegistration.handler(future)
}

/// Creates an immutable view whose owner keeps the FDB future alive.
///
/// FoundationDB owns returned pointers until the future is destroyed. Retaining
/// the future in the returned byte string preserves that lifetime without copying
/// payload bytes.
private func futureResultBytes(
    _ sourceBytes: UnsafePointer<UInt8>?,
    length: Int32,
    owner: FutureOwner
) throws -> FDB.ByteString {
    guard length >= 0 else {
        throw FDBError(.invalidAPICall)
    }
    guard length > 0 else {
        return FDB.ByteString(
            sharing: nil,
            count: 0,
            retaining: owner
        )
    }
    guard sourceBytes != nil else {
        throw FDBError(.invalidAPICall)
    }
    return FDB.ByteString(
        sharing: sourceBytes,
        count: Int(length),
        retaining: owner
    )
}

/// A result type for futures that return no data (void operations).
///
/// Used for operations like transaction commits that complete successfully
/// but don't return any specific value.
struct CompletionResultDecoder: ResultDecoder {
    /// Extracts a void result from the future (always succeeds if no error).
    ///
    /// - Parameter future: The C future to check for errors.
    /// - Returns: Nothing after successful completion.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws {
        _ = owner
        let errorCode = fdb_future_get_error(future)
        if errorCode != 0 {
            throw FDBError(code: errorCode)
        }

    }
}

/// A result type for futures that return version numbers.
///
/// Used for operations that return transaction version stamps or read versions.
struct VersionResultDecoder: ResultDecoder {
    /// Extracts a version from the future.
    ///
    /// - Parameter future: The C future containing the version.
    /// - Returns: The extracted FoundationDB version.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws -> FDB.Version {
        _ = owner
        var version: FDB.Version = 0
        let errorCode = fdb_future_get_int64(future, &version)
        if errorCode != 0 {
            throw FDBError(code: errorCode)
        }
        return version
    }
}

/// A result type for futures that return 64-bit integer values.
///
/// Used for operations that return size estimates or counts.
struct Int64ResultDecoder: ResultDecoder {
    /// Extracts an Int64 from the future.
    ///
    /// - Parameter future: The C future containing the integer.
    /// - Returns: The extracted integer.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws -> Int64 {
        _ = owner
        var value: Int64 = 0
        let errorCode = fdb_future_get_int64(future, &value)
        if errorCode != 0 {
            throw FDBError(code: errorCode)
        }
        return value
    }
}

/// A result type for futures that return key data.
///
/// Used for operations like key selectors that resolve to actual keys.
struct KeyResultDecoder: ResultDecoder {
    /// Extracts a key from the future.
    ///
    /// - Parameter future: The C future containing the key data.
    /// - Returns: An owner-backed view of the extracted key.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws -> FDB.ByteString {
        var keyStorage: UnsafePointer<UInt8>?
        var keyLength: Int32 = 0

        let errorCode = fdb_future_get_key(future, &keyStorage, &keyLength)
        if errorCode != 0 {
            throw FDBError(code: errorCode)
        }

        return try futureResultBytes(
            keyStorage,
            length: keyLength,
            owner: owner
        )
    }
}

/// A result type for futures that return value data.
///
/// Used for get operations that retrieve values associated with keys.
struct ValueResultDecoder: ResultDecoder {
    /// Extracts a value from the future.
    ///
    /// - Parameter future: The C future containing the value data.
    /// - Returns: An owner-backed value view, or `nil` when no value exists.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws -> FDB.ByteString? {
        var present: Int32 = 0
        var valueStorage: UnsafePointer<UInt8>?
        var valueLength: Int32 = 0

        let errorCode = fdb_future_get_value(
            future,
            &present,
            &valueStorage,
            &valueLength
        )
        if errorCode != 0 {
            throw FDBError(code: errorCode)
        }

        if present != 0 {
            return try futureResultBytes(
                valueStorage,
                length: valueLength,
                owner: owner
            )
        }

        return nil
    }
}

/// A result type for futures that return key-value ranges.
///
/// Used for range operations that retrieve multiple key-value pairs along
/// with information about whether more data is available.
public struct RangeBatch: Sendable {
    /// The array of key-value pairs returned by the range operation.
    ///
    /// Records decoded from a FoundationDB future are zero-copy views. Keeping
    /// any record alive retains the complete FoundationDB range result. Long-lived
    /// consumers should call `copyBytes()` at their explicit ownership boundary.
    public let records: FDB.KeyValueArray

    /// Indicates whether there are more records beyond this result.
    public let hasMore: Bool

    /// Creates an owned range result.
    ///
    /// This initializer is useful for transaction implementations that already
    /// own their key/value records and therefore do not decode an FDB future.
    public init(records: FDB.KeyValueArray, hasMore: Bool) {
        self.records = records
        self.hasMore = hasMore
    }
}

/// Decodes a FoundationDB range future into owner-backed row views.
struct RangeBatchResultDecoder: ResultDecoder {
    /// Extracts key-value pairs from a range future.
    ///
    /// - Parameter future: The C future containing the key-value array.
    /// - Returns: A `RangeBatch` with the extracted records and has-more flag.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws -> RangeBatch {
        var sourceRecords: UnsafePointer<FDBKeyValue>?
        var recordCount: Int32 = 0
        var hasMore: Int32 = 0

        let errorCode = fdb_future_get_keyvalue_array(
            future,
            &sourceRecords,
            &recordCount,
            &hasMore
        )
        if errorCode != 0 {
            throw FDBError(code: errorCode)
        }

        guard recordCount >= 0 else {
            throw FDBError(.invalidAPICall)
        }
        guard recordCount > 0 else {
            return RangeBatch(records: [], hasMore: hasMore != 0)
        }
        guard let sourceRecords else {
            throw FDBError(.invalidAPICall)
        }

        var keyValueArray: FDB.KeyValueArray = []
        keyValueArray.reserveCapacity(Int(recordCount))
        for index in 0 ..< Int(recordCount) {
            let sourceRecord = sourceRecords[index]
            let key = try futureResultBytes(
                sourceRecord.key,
                length: sourceRecord.key_length,
                owner: owner
            )
            let value = try futureResultBytes(
                sourceRecord.value,
                length: sourceRecord.value_length,
                owner: owner
            )
            keyValueArray.append((key, value))
        }

        return RangeBatch(records: keyValueArray, hasMore: hasMore != 0)
    }
}

/// A result type for futures that return arrays of keys.
///
/// Used for operations like get range split points that return multiple keys.
struct KeyCollectionResultDecoder: ResultDecoder {
    /// Extracts an array of keys from the future.
    ///
    /// - Parameter future: The C future containing the key array.
    /// - Returns: Owner-backed views of the extracted keys.
    /// - Throws: `FDBError` if the future contains an error.
    static func decode(
        from future: FutureHandle,
        retaining owner: FutureOwner
    ) throws -> [FDB.ByteString] {
        var sourceKeys: UnsafePointer<FDBKey>?
        var keyCount: Int32 = 0

        let errorCode = fdb_future_get_key_array(future, &sourceKeys, &keyCount)
        if errorCode != 0 {
            throw FDBError(code: errorCode)
        }

        guard keyCount >= 0 else {
            throw FDBError(.invalidAPICall)
        }
        guard keyCount > 0 else {
            return []
        }
        guard let sourceKeys else {
            throw FDBError(.invalidAPICall)
        }

        var keyArray: [FDB.ByteString] = []
        keyArray.reserveCapacity(Int(keyCount))
        for index in 0 ..< Int(keyCount) {
            let sourceKey = sourceKeys[index]
            let key = try futureResultBytes(
                sourceKey.key,
                length: sourceKey.key_length,
                owner: owner
            )
            keyArray.append(key)
        }

        return keyArray
    }
}
