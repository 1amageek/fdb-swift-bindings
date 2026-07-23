/*
 * Tuple+Versionstamp.swift
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

// MARK: - Versionstamp Support

extension Tuple {

    /// Pack tuple with an incomplete versionstamp and append offset
    ///
    /// This method packs a tuple that contains exactly one incomplete versionstamp,
    /// and appends the byte offset where the versionstamp appears.
    ///
    /// The offset is always 4 bytes (uint32, little-endian) as per API version 520+.
    /// API versions prior to 520 used 2-byte offsets but are no longer supported.
    ///
    /// The resulting key can be used with `SET_VERSIONSTAMPED_KEY` atomic operation.
    /// At commit time, FoundationDB will replace the 10-byte placeholder with the
    /// actual transaction versionstamp.
    ///
    /// - Parameter prefix: Optional prefix bytes to prepend (default: empty)
    /// - Returns: Packed bytes with offset appended
    /// - Throws: A typed `TupleError` when the tuple does not contain exactly
    ///   one incomplete versionstamp or its offset cannot be represented.
    ///
    /// Example usage:
    /// ```swift
    /// let vs = Versionstamp.incomplete(userVersion: 0)
    /// let tuple = Tuple("user", 12345, vs)
    /// let key = try tuple.packWithVersionstamp()
    ///
    /// transaction.atomicOp(
    ///     key: key,
    ///     param: [],
    ///     mutationType: .setVersionstampedKey
    /// )
    /// ```
    public func packWithVersionstamp(prefix: FDB.Bytes = []) throws -> FDB.Bytes {
        var packed = prefix
        var versionstampPosition: Int? = nil
        var incompleteCount = 0

        // Encode each element and track incomplete versionstamp position
        for element in elements {
            if let vs = element as? Versionstamp {
                if !vs.isComplete {
                    incompleteCount += 1
                    if versionstampPosition == nil {
                        // Position points to start of 10-byte transaction version
                        // (after type code byte and before the 10-byte placeholder)
                        versionstampPosition = packed.count + 1  // +1 for type code 0x33
                    }
                }
            }

            packed.append(contentsOf: element.encodeTuple())
        }

        guard incompleteCount > 0, let position = versionstampPosition else {
            throw TupleError.missingIncompleteVersionstamp
        }
        guard incompleteCount == 1 else {
            throw TupleError.multipleIncompleteVersionstamps
        }

        // Append offset based on API version
        // Currently defaults to API 520+ behavior (4-byte offset)
        // API < 520 used 2-byte offset, but is no longer supported

        // API >= 520: Use 4-byte offset (uint32, little-endian)
        guard let offset = UInt32(exactly: position) else {
            throw TupleError.versionstampOffsetOverflow(position)
        }

        withUnsafeBytes(of: offset.littleEndian) { packed.append(contentsOf: $0) }

        return packed
    }

    /// Whether the tuple contains an incomplete versionstamp.
    public var containsIncompleteVersionstamp: Bool {
        elements.contains { element in
            if let vs = element as? Versionstamp {
                return !vs.isComplete
            }
            return false
        }
    }

    /// Number of incomplete versionstamps in the tuple.
    public var incompleteVersionstampCount: Int {
        elements.count { element in
            if let vs = element as? Versionstamp {
                return !vs.isComplete
            }
            return false
        }
    }

    /// Validate tuple for use with packWithVersionstamp()
    /// - Throws: A typed `TupleError` unless exactly one versionstamp is incomplete.
    public func validateForVersionstamp() throws {
        let incompleteCount = incompleteVersionstampCount

        guard incompleteCount > 0 else {
            throw TupleError.missingIncompleteVersionstamp
        }
        guard incompleteCount == 1 else {
            throw TupleError.multipleIncompleteVersionstamps
        }
    }
}
