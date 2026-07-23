/*
 * FDB+AsyncKVSequence.swift
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

import Synchronization

/// Provides async sequence support for iterating over FoundationDB key-value ranges.
///
/// This file implements efficient streaming iteration over large result sets from FoundationDB
/// using Swift's AsyncSequence protocol with demand-driven page reads.

extension FDB {
    /// An asynchronous sequence that efficiently streams key-value pairs from FoundationDB.
    ///
    /// `AsyncKVSequence` provides an asynchronous way to iterate over large result sets from
    /// FoundationDB range queries without loading all data into memory at once. It implements
    /// demand-driven reads so database I/O cannot outlive a returned element.
    ///
    /// ## Usage
    ///
    /// ```swift
    /// // Basic forward scan
    /// for try await (key, value) in transaction.getRange(
    ///     from: .firstGreaterOrEqual("user:"),
    ///     to: .firstGreaterOrEqual("user;")
    /// ) {
    ///     print(key, value)
    /// }
    ///
    /// // Reverse scan with limit
    /// for try await (key, value) in transaction.getRange(
    ///     from: .firstGreaterOrEqual("user:"),
    ///     to: .firstGreaterOrEqual("user;"),
    ///     limit: 10,
    ///     reverse: true
    /// ) {
    ///     print(key, value)
    /// }
    /// ```
    ///
    /// ## Performance Characteristics
    ///
    /// - **Streaming**: Results are processed as they arrive, not buffered entirely in memory
    /// - **Deterministic lifecycle**: No next-page I/O runs while the caller owns an element
    /// - **Configurable Streaming Mode**: Use `.wantAll` for bulk reads, `.iterator` for balanced reads
    /// - **Snapshot Consistency**: Supports both snapshot and non-snapshot reads
    ///
    /// ## Implementation Notes
    ///
    /// The sequence uses an optimized async iterator that:
    /// 1. Fetches a page only when `next()` needs one
    /// 2. Completes database I/O before returning an element
    /// 3. Leaves no in-flight read after `next()` returns
    /// 4. Tracks iteration count internally for ITERATOR streaming mode optimization
    public struct AsyncKVSequence: AsyncSequence, Sendable {
        public typealias Element = (ByteString, ByteString)

        /// The transaction used for range queries
        let transaction: TransactionProtocol
        /// Starting key selector for the range
        let beginSelector: FDB.KeySelector
        /// Ending key selector for the range (exclusive)
        let endSelector: FDB.KeySelector
        /// Maximum number of results to return (0 = unlimited)
        let limit: Int
        /// Whether to scan in reverse order
        let reverse: Bool
        /// Whether to use snapshot reads
        let snapshot: Bool
        /// How to batch/stream results from the server
        let streamingMode: StreamingMode

        /// Creates a new AsyncKVSequence.
        ///
        /// - Parameters:
        ///   - transaction: The transaction to use for range queries.
        ///   - beginSelector: Starting key selector for the range.
        ///   - endSelector: Ending key selector for the range (exclusive).
        ///   - limit: Maximum results to return (0 = unlimited).
        ///   - reverse: Whether to scan in reverse order.
        ///   - snapshot: Whether to use snapshot isolation.
        ///   - streamingMode: How to batch/stream results.
        public init(
            transaction: TransactionProtocol,
            beginSelector: FDB.KeySelector,
            endSelector: FDB.KeySelector,
            limit: Int = 0,
            reverse: Bool = false,
            snapshot: Bool = false,
            streamingMode: StreamingMode = .iterator
        ) {
            self.transaction = transaction
            self.beginSelector = beginSelector
            self.endSelector = endSelector
            self.limit = limit
            self.reverse = reverse
            self.snapshot = snapshot
            self.streamingMode = streamingMode
        }

        /// Creates a new async iterator for this sequence.
        ///
        /// - Returns: A new `AsyncIterator` configured for this sequence
        public func makeAsyncIterator() -> AsyncIterator {
            AsyncIterator(
                transaction: transaction,
                beginSelector: beginSelector,
                endSelector: endSelector,
                limit: limit,
                reverse: reverse,
                snapshot: snapshot,
                streamingMode: streamingMode
            )
        }

        /// Demand-driven async range iterator.
        ///
        /// Swift `AsyncSequence` has no asynchronous early-exit hook. Fetching a
        /// speculative next page would therefore allow database I/O to outlive a
        /// caller's `break` or `return`. This iterator keeps one page and opens
        /// the next page only from a subsequent `next()` call.
        ///
        /// ## Thread Safety
        ///
        /// This iterator is **not** thread-safe. Each iterator should be used by a single task.
        /// Multiple iterators can be created from the same sequence for concurrent processing.
        ///
        public final class AsyncIterator: AsyncIteratorProtocol, Sendable {
            private let iterationState: RangeIterationState

            /// Initializes the iterator without issuing database I/O.
            ///
            /// - Parameters:
            ///   - transaction: The transaction to use for range queries
            ///   - beginSelector: Starting key selector for the range
            ///   - endSelector: Ending key selector for the range (exclusive)
            ///   - limit: Maximum results to return (0 = unlimited)
            ///   - reverse: Whether to scan in reverse order
            ///   - snapshot: Whether to use snapshot reads
            ///   - streamingMode: How to batch/stream results
            init(
                transaction: TransactionProtocol,
                beginSelector: FDB.KeySelector,
                endSelector: FDB.KeySelector,
                limit: Int,
                reverse: Bool,
                snapshot: Bool,
                streamingMode: StreamingMode
            ) {
                self.iterationState = RangeIterationState(
                    transaction: transaction,
                    beginSelector: beginSelector,
                    endSelector: endSelector,
                    limit: limit,
                    reverse: reverse,
                    snapshot: snapshot,
                    streamingMode: streamingMode
                )
            }

            /// Completes iterator cleanup.
            ///
            /// Reads are demand-driven and finish before `next()` returns, so no
            /// background native operation remains to join.
            public func finish(
                isolation actor: isolated (any Actor)? = #isolation
            ) async throws {
                try await iterationState.finish()
            }

            /// Returns the next key-value pair in the sequence.
            ///
            /// This method implements demand-driven batch iteration:
            ///
            /// 1. If current batch has items, return next item immediately
            /// 2. If current batch is exhausted, fetch the next page
            /// 3. Return after the database read has completed
            ///
            /// - Returns: The next key-value pair, or `nil` if sequence is exhausted
            /// - Throws: `FDBError` if the database operation fails
            public func next() async throws -> Element? {
                try await iterationState.next()
            }
        }
    }
}

private actor RangeIterationState {
    private enum Lifecycle {
        case idle
        case reading(RangeReadCompletion, finishRequested: Bool)
        case finished
    }

    private let transaction: any TransactionProtocol
    private var nextBeginSelector: FDB.KeySelector
    private var nextEndSelector: FDB.KeySelector
    private let limit: Int
    private let reverse: Bool
    private let snapshot: Bool
    private let streamingMode: FDB.StreamingMode
    private var currentBatch: RangeBatch = .init(records: [], hasMore: true)
    private var currentIndex = 0
    private var totalReturned = 0
    private var iteration = 1
    private var lifecycle = Lifecycle.idle

    init(
        transaction: any TransactionProtocol,
        beginSelector: FDB.KeySelector,
        endSelector: FDB.KeySelector,
        limit: Int,
        reverse: Bool,
        snapshot: Bool,
        streamingMode: FDB.StreamingMode
    ) {
        self.transaction = transaction
        self.nextBeginSelector = beginSelector
        self.nextEndSelector = endSelector
        self.limit = limit
        self.reverse = reverse
        self.snapshot = snapshot
        self.streamingMode = streamingMode
    }

    func next() async throws -> (FDB.ByteString, FDB.ByteString)? {
        while true {
            switch lifecycle {
            case .finished:
                return nil
            case .reading(let completion, _):
                try await completion.wait().get()
            case .idle:
                if limitReached || isExhausted {
                    lifecycle = .finished
                    return nil
                }
                if !currentBatchExhausted {
                    let keyValue = currentBatch.records[currentIndex]
                    currentIndex += 1
                    totalReturned += 1
                    return keyValue
                }
                try await fetchNextBatch()
            }
        }
    }

    func finish() async throws {
        while true {
            switch lifecycle {
            case .idle:
                lifecycle = .finished
                return
            case .finished:
                return
            case .reading(let completion, false):
                lifecycle = .reading(completion, finishRequested: true)
                try await completion.wait().get()
            case .reading(let completion, true):
                try await completion.wait().get()
            }
        }
    }

    private var isExhausted: Bool {
        currentBatchExhausted && !currentBatch.hasMore
    }

    private var currentBatchExhausted: Bool {
        currentIndex >= currentBatch.records.count
    }

    private var limitReached: Bool {
        limit > 0 && totalReturned >= limit
    }

    private var remainingLimit: Int {
        guard limit > 0 else { return 0 }
        return Swift.max(0, limit - totalReturned)
    }

    private func fetchNextBatch() async throws {
        let completion = RangeReadCompletion()
        lifecycle = .reading(completion, finishRequested: false)
        do {
            let nextBatch = try await transaction.readRangeBatch(
                from: nextBeginSelector,
                to: nextEndSelector,
                limit: remainingLimit,
                targetBytes: 0,
                streamingMode: streamingMode,
                iteration: iteration,
                reverse: reverse,
                snapshot: snapshot
            )
            iteration += 1

            guard case .reading(let activeCompletion, let finishRequested) = lifecycle,
                  activeCompletion === completion else {
                preconditionFailure("Range read lost its serialized lifecycle state")
            }
            if finishRequested {
                lifecycle = .finished
                completion.resolve(.success(()))
                return
            }

            assert(currentIndex >= currentBatch.records.count)
            currentBatch = nextBatch
            currentIndex = 0
            updateContinuationSelector(from: nextBatch)
            lifecycle = .idle
            completion.resolve(.success(()))
        } catch {
            lifecycle = .finished
            completion.resolve(.failure(error))
            throw error
        }
    }

    private func updateContinuationSelector(
        from nextBatch: borrowing RangeBatch
    ) {
        guard !currentBatch.records.isEmpty,
              currentBatch.hasMore,
              !limitReached,
              let lastKey = nextBatch.records.last?.0 else {
            return
        }
        if reverse {
            nextEndSelector = .firstGreaterOrEqual(lastKey)
        } else {
            nextBeginSelector = .firstGreaterThan(lastKey)
        }
    }
}

private final class RangeReadCompletion: Sendable {
    private struct MutableState: Sendable {
        var result: Result<Void, any Error>?
        var waiters: [CheckedContinuation<Result<Void, any Error>, Never>] = []
    }

    private let state = Mutex(MutableState())

    func wait() async -> Result<Void, any Error> {
        let completed = state.withLock { $0.result }
        if let completed {
            return completed
        }
        return await withCheckedContinuation { continuation in
            let racedResult = state.withLock { state
                -> Result<Void, any Error>? in
                if let result = state.result {
                    return result
                }
                state.waiters.append(continuation)
                return nil
            }
            if let racedResult {
                continuation.resume(returning: racedResult)
            }
        }
    }

    func resolve(_ result: Result<Void, any Error>) {
        let waiters = state.withLock { state in
            precondition(state.result == nil, "Range read completed more than once")
            state.result = result
            let waiters = state.waiters
            state.waiters.removeAll(keepingCapacity: false)
            return waiters
        }
        for waiter in waiters {
            waiter.resume(returning: result)
        }
    }
}
