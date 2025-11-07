/*
 * HighContentionAllocator.swift
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

import Foundation

/// High Contention Allocator for efficiently allocating unique directory prefixes
///
/// The HCA uses a window-based allocation strategy with dynamic window sizing
/// to balance key shortness with contention handling. This is the official
/// algorithm used across all FoundationDB language bindings.
///
/// **Algorithm**:
/// 1. Find appropriate allocation window with dynamic sizing
/// 2. Randomly select candidate prefix within window
/// 3. Check if candidate was recently allocated
/// 4. Use write conflict keys to prevent concurrent allocation
///
/// **Window Sizes**:
/// - start < 255: window = 64
/// - start < 65535: window = 1024
/// - start >= 65535: window = 8192
public final class HighContentionAllocator: Sendable {

    // MARK: - Properties

    /// Database instance
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Counters subspace for tracking allocation counts
    private let counters: Subspace

    /// Recent subspace for tracking recently allocated prefixes
    private let recent: Subspace

    // MARK: - Initialization

    /// Initialize High Contention Allocator
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - subspace: Root subspace for HCA (typically nodeSubspace["hca"])
    public init(database: any DatabaseProtocol, subspace: Subspace) {
        self.database = database
        self.counters = subspace.subspace(0)
        self.recent = subspace.subspace(1)
    }

    // MARK: - Allocation

    /// Allocate a unique prefix
    ///
    /// - Parameter transaction: Transaction to use for allocation
    /// - Returns: Allocated prefix as FDB.Bytes
    /// - Throws: DirectoryError if allocation fails
    public func allocate(transaction: any TransactionProtocol) async throws -> FDB.Bytes {
        var start: Int64 = 0
        var windowAdvanced = false

        while true {
            // 1. Get latest counter start value
            let latestCounter = try await getLatestCounter(transaction: transaction)
            if let counter = latestCounter {
                start = counter
            }

            // 2. Find appropriate allocation window
            let window = windowSize(start: start)

            while true {
                // Increment counter for current window
                let counterKey = counters.pack(Tuple(start))
                transaction.atomicOp(
                    key: Array(counterKey),
                    param: littleEndianInt64(1),
                    mutationType: .add
                )

                // Read current count
                let countData = try await transaction.getValue(for: Array(counterKey), snapshot: false)
                let count = countData.map { decodeInt64($0) } ?? 1

                // Check if window is less than half full
                if count * 2 < window {
                    break  // Found suitable window
                }

                // Window is too full - advance to next window
                if windowAdvanced {
                    // Clear old window data to optimize space
                    // Use setNextWriteNoWriteConflictRange() to avoid conflicts on old data
                    let oldCountersBegin = counters.pack(Tuple(start))
                    let oldCountersEnd = counters.pack(Tuple(start + 1))
                    try transaction.setNextWriteNoWriteConflictRange()
                    transaction.clearRange(beginKey: Array(oldCountersBegin), endKey: Array(oldCountersEnd))

                    let oldRecentBegin = recent.pack(Tuple(start))
                    let oldRecentEnd = recent.pack(Tuple(start + window))
                    try transaction.setNextWriteNoWriteConflictRange()
                    transaction.clearRange(beginKey: Array(oldRecentBegin), endKey: Array(oldRecentEnd))
                }

                start += window
                windowAdvanced = true
            }

            // 3. Randomly select candidate within window
            let candidate = start + Int64.random(in: 0..<window)

            // 4. Check if candidate was recently allocated
            let candidateKey = recent.pack(Tuple(candidate))
            let candidateValue = try await transaction.getValue(for: Array(candidateKey), snapshot: false)

            if candidateValue == nil {
                // Candidate is available
                // Mark as used without write conflict detection, then add explicit conflict
                try transaction.setNextWriteNoWriteConflictRange()
                transaction.setValue([0x01], for: Array(candidateKey))
                try transaction.addWriteConflictKey(Array(candidateKey))

                // Return packed candidate as prefix
                let packedCandidate = Tuple(candidate).encode()
                return packedCandidate
            }

            // 5. Check if window has advanced (another transaction moved forward)
            let currentLatest = try await getLatestCounter(transaction: transaction)
            if let latest = currentLatest, latest > start {
                // Window advanced - restart from beginning
                continue
            }

            // Try another random candidate in same window
        }
    }

    // MARK: - Helper Methods

    /// Calculate window size based on start value
    ///
    /// Dynamic window sizing balances key shortness vs. contention handling:
    /// - Small values: smaller window (64) for shorter keys
    /// - Medium values: medium window (1024)
    /// - Large values: large window (8192) to handle high contention
    ///
    /// - Parameter start: Current window start value
    /// - Returns: Window size
    private func windowSize(start: Int64) -> Int64 {
        if start < 255 {
            return 64
        } else if start < 65535 {
            return 1024
        } else {
            return 8192
        }
    }

    /// Get the latest counter start value
    ///
    /// Uses snapshot read to avoid unnecessary read conflicts on counter metadata.
    ///
    /// - Parameter transaction: Transaction to use
    /// - Returns: Latest counter start value, or nil if no counters exist
    private func getLatestCounter(transaction: any TransactionProtocol) async throws -> Int64? {
        let (_, end) = counters.range()

        // Get last key in counters subspace using lastLessThan selector
        // Use snapshot read to avoid read conflicts
        let sequence = transaction.getRange(
            beginSelector: .lastLessThan(end),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            // Check if key belongs to counters subspace before unpacking
            guard key.starts(with: counters.prefix) else {
                // Key is outside counters subspace, skip it
                continue
            }

            let tuple = try counters.unpack(Array(key))
            if let start = tuple[0] as? Int64 {
                return start
            }
        }

        return nil
    }

    /// Encode Int64 as little-endian bytes
    private func littleEndianInt64(_ value: Int64) -> FDB.Bytes {
        var val = value.littleEndian
        return withUnsafeBytes(of: &val) { Array($0) }
    }

    /// Decode Int64 from little-endian bytes
    private func decodeInt64(_ bytes: FDB.Bytes) -> Int64 {
        guard bytes.count == 8 else { return 0 }
        return bytes.withUnsafeBytes { $0.load(as: Int64.self).littleEndian }
    }
}
