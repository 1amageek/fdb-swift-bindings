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

/// Provides async sequence support for iterating over FoundationDB key-value ranges.
///
/// This file implements efficient streaming iteration over large result sets from FoundationDB
/// using Swift's AsyncSequence protocol with optimized background pre-fetching.

extension FDB {
    /// An asynchronous sequence that efficiently streams key-value pairs from FoundationDB.
    ///
    /// `AsyncKVSequence` provides a Swift-native way to iterate over large result sets from
    /// FoundationDB range queries without loading all data into memory at once. It implements
    /// background pre-fetching to minimize network latency and maximize throughput.
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
    /// - **Background Pre-fetching**: Next batch is fetched concurrently while processing current batch
    /// - **Configurable Streaming Mode**: Use `.wantAll` for bulk reads, `.iterator` for balanced reads
    /// - **Snapshot Consistency**: Supports both snapshot and non-snapshot reads
    ///
    /// ## Implementation Notes
    ///
    /// The sequence uses an optimized async iterator that:
    /// 1. Starts pre-fetching the next batch immediately upon initialization
    /// 2. Continues pre-fetching in background while serving current batch items
    /// 3. Only blocks when transitioning between batches if pre-fetch isn't complete
    /// 4. Tracks iteration count internally for ITERATOR streaming mode optimization
    public struct AsyncKVSequence: AsyncSequence {
        public typealias Element = (Bytes, Bytes)

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
        /// The iterator begins background pre-fetching immediately upon creation to minimize
        /// latency for the first `next()` call.
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

        /// High-performance async iterator with background pre-fetching.
        ///
        /// This iterator implements an optimized batching strategy:
        ///
        /// 1. **Immediate Pre-fetch**: Starts fetching the first batch during initialization
        /// 2. **Background Pre-fetch**: While serving items from current batch, pre-fetches next batch
        /// 3. **Minimal Blocking**: Only blocks when current batch is exhausted and next isn't ready
        ///
        /// ## Performance Benefits
        ///
        /// - **Overlapped I/O**: Network requests happen concurrently with data processing
        /// - **Reduced Latency**: Pre-fetching hides network round-trip time
        /// - **Memory Efficient**: Only keeps 1-2 batches in memory at any time
        ///
        /// ## Thread Safety
        ///
        /// This iterator is **not** thread-safe. Each iterator should be used by a single task.
        /// Multiple iterators can be created from the same sequence for concurrent processing.
        ///
        /// ## Lifecycle Management
        ///
        /// This is a `class` (not `struct`) to enable automatic cleanup via `deinit`.
        /// When iteration is abandoned (e.g., via `break` in a `for await` loop),
        /// the background pre-fetch Task is automatically cancelled, preventing
        /// "Operation issued while a commit was outstanding" errors.
        public final class AsyncIterator: AsyncIteratorProtocol {
            /// Transaction used for all range queries
            private let transaction: TransactionProtocol
            /// Key selector for the next batch (forward: begin, reverse: end)
            private var nextBeginSelector: FDB.KeySelector
            /// Key selector for the end boundary (forward: constant, reverse: updated)
            private var nextEndSelector: FDB.KeySelector
            /// Total limit for results (0 = unlimited)
            private let limit: Int
            /// Whether to scan in reverse order
            private let reverse: Bool
            /// Whether to use snapshot reads
            private let snapshot: Bool
            /// Streaming mode for batching
            private let streamingMode: StreamingMode

            // MARK: - Internal State

            /// Current batch of records being served
            private var currentBatch: ResultRange = .init(records: [], more: true)
            /// Index of next item to return from current batch
            private var currentIndex: Int = 0
            /// Total number of items returned so far
            private var totalReturned: Int = 0
            /// Current iteration number for ITERATOR streaming mode
            private var iteration: Int = 1
            /// Background task pre-fetching the next batch
            private var preFetchTask: Task<ResultRange?, Error>?

            /// Returns `true` when all available data has been consumed
            private var isExhausted: Bool {
                currentBatchExhausted && !currentBatch.more
            }

            /// Returns `true` when current batch has no more items to serve
            private var currentBatchExhausted: Bool {
                currentIndex >= currentBatch.records.count
            }

            /// Returns `true` if limit has been reached
            private var limitReached: Bool {
                limit > 0 && totalReturned >= limit
            }

            /// Remaining items to fetch considering limit
            private var remainingLimit: Int {
                guard limit > 0 else { return 0 }
                return Swift.max(0, limit - totalReturned)
            }

            /// Cancels any running pre-fetch task when the iterator is deallocated.
            ///
            /// This ensures that abandoning iteration (e.g., via `break`) properly cleans up
            /// background tasks, preventing conflicts with subsequent transaction operations.
            deinit {
                preFetchTask?.cancel()
            }

            /// Initializes the iterator and immediately starts pre-fetching the first batch.
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
                self.transaction = transaction
                self.nextBeginSelector = beginSelector
                self.nextEndSelector = endSelector
                self.limit = limit
                self.reverse = reverse
                self.snapshot = snapshot
                self.streamingMode = streamingMode

                // Start fetching immediately to minimize latency on first next() call
                startBackgroundPreFetch()
            }

            /// Cancels the pre-fetch task and waits for it to complete.
            ///
            /// Call this method to ensure no in-flight FDB operations remain before
            /// the transaction is committed. This is critical when wrapping this
            /// iterator in another AsyncSequence that may return `nil` without
            /// calling `next()` on the inner iterator (e.g., due to its own limit check).
            ///
            /// Combined with `withTaskCancellationHandler` in `Future.getAsync()`,
            /// the C future is cancelled immediately, so this await completes quickly.
            public func finish() async {
                await cancelAndWaitForPreFetch()
            }

            private func cancelAndWaitForPreFetch() async {
                guard let task = preFetchTask else { return }
                preFetchTask = nil
                task.cancel()
                _ = try? await task.value
            }

            /// Returns the next key-value pair in the sequence.
            ///
            /// This method implements the core iteration logic with optimal performance:
            ///
            /// 1. If current batch has items, return next item immediately
            /// 2. If current batch is exhausted, wait for pre-fetched batch
            /// 3. Continue pre-fetching next batch in background
            ///
            /// The method only blocks on network I/O when transitioning between batches
            /// and the next batch isn't ready yet.
            ///
            /// - Returns: The next key-value pair, or `nil` if sequence is exhausted
            /// - Throws: `FDBError` if the database operation fails
            public func next() async throws -> Element? {
                // Check if we've hit the limit
                if limitReached {
                    await cancelAndWaitForPreFetch()
                    return nil
                }

                if isExhausted {
                    await cancelAndWaitForPreFetch()
                    return nil
                }

                if currentBatchExhausted {
                    try await updateCurrentBatch()
                }

                if currentBatchExhausted {
                    // If last fetch didn't bring any new records, we've read everything.
                    await cancelAndWaitForPreFetch()
                    return nil
                }

                let keyValue = currentBatch.records[currentIndex]
                currentIndex += 1
                totalReturned += 1
                return keyValue
            }

            /// Updates the current batch with pre-fetched data and starts next pre-fetch.
            ///
            /// This method is called when the current batch is exhausted and we need to
            /// move to the next batch. It waits for the background pre-fetch task to complete,
            /// updates the iterator state, and starts pre-fetching the subsequent batch.
            ///
            /// - Throws: `FDBError` if the pre-fetch operation failed
            private func updateCurrentBatch() async throws {
                guard let nextBatch = try await preFetchTask?.value else {
                    throw FDBError(.clientError)
                }

                assert(currentIndex >= currentBatch.records.count)
                currentBatch = nextBatch
                currentIndex = 0

                if !currentBatch.records.isEmpty, currentBatch.more, !limitReached {
                    let lastKey = nextBatch.records.last!.0
                    if reverse {
                        // Reverse mode: scan from end toward begin
                        // Update end selector to continue from last returned key
                        // Reference: FDB Python binding uses firstGreaterOrEqual(lastKey)
                        nextEndSelector = FDB.KeySelector.firstGreaterOrEqual(lastKey)
                    } else {
                        // Forward mode: scan from begin toward end
                        // Update begin selector to continue after last returned key
                        nextBeginSelector = FDB.KeySelector.firstGreaterThan(lastKey)
                    }
                    startBackgroundPreFetch()
                } else {
                    preFetchTask = nil
                }
            }

            /// Starts background pre-fetching of the next batch.
            ///
            /// This method creates a background Task that performs the next range query
            /// concurrently. The task captures all necessary values to avoid reference
            /// cycles and ensure thread safety.
            ///
            /// The pre-fetch runs independently and can complete while the iterator
            /// is serving items from the current batch, minimizing blocking time
            /// during batch transitions.
            private func startBackgroundPreFetch() {
                let capturedTransaction = transaction
                let capturedBeginSelector = nextBeginSelector
                let capturedEndSelector = nextEndSelector
                let capturedLimit = remainingLimit
                let capturedReverse = reverse
                let capturedSnapshot = snapshot
                let capturedStreamingMode = streamingMode
                let capturedIteration = iteration

                preFetchTask = Task {
                    return try await capturedTransaction.getRangeNative(
                        beginSelector: capturedBeginSelector,
                        endSelector: capturedEndSelector,
                        limit: capturedLimit,
                        targetBytes: 0,
                        streamingMode: capturedStreamingMode,
                        iteration: capturedIteration,
                        reverse: capturedReverse,
                        snapshot: capturedSnapshot
                    )
                }

                // Increment iteration for next fetch (ITERATOR mode optimization)
                iteration += 1
            }
        }
    }
}
