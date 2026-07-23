/*
 * Error.swift
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

public enum FDBErrorCode: Int32, CaseIterable, Sendable {
    case operationTimedOut = 1004
    case transactionTooOld = 1007
    case futureVersion = 1009
    case notCommitted = 1020
    case commitUnknownResult = 1021
    case idempotencyStatusUnknown = 1022
    case transactionCancelled = 1025
    case transactionTimedOut = 1031
    case processBehind = 1037
    case operationCancelled = 1101
    case tagThrottled = 1213
    case invalidAPICall = 2000
    case transactionTooLarge = 2101
    case keyTooLarge = 2102
    case valueTooLarge = 2103
    case apiVersionAlreadySet = 2201
    case internalError = 4100
    case unknownError = 9999
}

/// Whether replaying a transaction body is known to be safe.
public enum FDBRetryDisposition: Sendable, Equatable {
    /// FoundationDB guarantees that the transaction did not commit.
    case retryableNotCommitted

    /// The transaction may have committed. Replay requires an idempotency
    /// decision above the binding layer.
    case maybeCommitted

    /// The error is not retryable by the transaction runner.
    case never
}

public struct FDBError: Error, CustomStringConvertible {
    public let code: Int32

    public init(code: Int32) {
        self.code = code
    }

    public init(_ errorCode: FDBErrorCode) {
        code = errorCode.rawValue
    }

    public var description: String {
        guard let errorCString = fdb_get_error(code) else {
            return "Unknown FDB error: \(code)"
        }
        return String(cString: errorCString)
    }

    public var knownCode: FDBErrorCode? {
        FDBErrorCode(rawValue: code)
    }

    public var retryDisposition: FDBRetryDisposition {
        if matches(.maybeCommitted) {
            return .maybeCommitted
        }
        if matches(.retryableNotCommitted) {
            return .retryableNotCommitted
        }
        return .never
    }

    private func matches(_ predicate: FDB.ErrorPredicate) -> Bool {
        fdb_error_predicate(Int32(predicate.rawValue), code) != 0
    }
}
