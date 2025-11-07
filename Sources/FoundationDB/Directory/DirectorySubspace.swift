/*
 * DirectorySubspace.swift
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

/// A subspace returned from Directory operations
///
/// DirectorySubspace extends Subspace with directory metadata (path and layer).
/// It provides the same data operations as Subspace (pack/unpack), plus
/// reversible path information.
///
/// **Design**: DirectorySubspace = Subspace + reversible path where path: [String]
///
/// **Basic Usage**:
/// ```swift
/// let userDir = try await directoryLayer.createOrOpen(path: ["app", "users"])
///
/// // Use for data operations
/// let key = userDir.pack(Tuple("user123"))
/// try await transaction.set(key, value: userData)
///
/// // Access path information
/// print(userDir.path)  // ["app", "users"]
/// print(userDir.prefix)  // HCA-allocated prefix
/// ```
///
/// **Note**: Directory operations (createOrOpen, list, remove, etc.) should be
/// performed through DirectoryLayer, not DirectorySubspace.
public struct DirectorySubspace: Sendable {

    // MARK: - Properties

    /// Short prefix assigned by Directory Layer (via HCA)
    public let subspace: Subspace

    /// Full path of this directory
    public let path: [String]

    /// Layer type of this directory
    public let type: DirectoryType?

    /// Whether this directory is a partition
    public var isPartition: Bool {
        type == .partition
    }

    /// Convenience: Access the prefix directly
    public var prefix: FDB.Bytes {
        subspace.prefix
    }

    // MARK: - Initialization

    /// Initialize DirectorySubspace
    ///
    /// - Parameters:
    ///   - prefix: Prefix assigned by Directory Layer (via HCA)
    ///   - path: Full directory path
    ///   - layer: Layer type (optional)
    public init(
        prefix: FDB.Bytes,
        path: [String],
        type: DirectoryType?
    ) {
        self.subspace = Subspace(prefix: prefix)
        self.path = path
        self.type = type
    }

    /// Initialize DirectorySubspace from Subspace (for backward compatibility)
    ///
    /// - Parameters:
    ///   - subspace: Subspace with the prefix assigned by Directory Layer
    ///   - path: Full directory path
    ///   - layer: Layer type (optional)
    public init(
        subspace: Subspace,
        path: [String],
        type: DirectoryType?
    ) {
        self.subspace = subspace
        self.path = path
        self.type = type
    }

    // MARK: - Subspace Operations

    /// Pack tuple into key
    ///
    /// - Parameter tuple: Tuple to pack
    /// - Returns: Key with prefix
    public func pack(_ tuple: Tuple) -> Data {
        Data(self.subspace.pack(tuple))
    }

    /// Unpack key into tuple
    ///
    /// - Parameter key: Key to unpack
    /// - Returns: Unpacked tuple
    /// - Throws: If key doesn't belong to this subspace
    public func unpack(_ key: Data) throws -> Tuple {
        try self.subspace.unpack(Array(key))
    }

    /// Create a subspace
    ///
    /// - Parameter tuple: Subspace tuple
    /// - Returns: New subspace
    public func subspace(_ tuple: Tuple) -> Subspace {
        self.subspace.subspace(tuple)
    }

    /// Get key range for this subspace
    ///
    /// - Returns: Begin and end keys
    public func range() -> (begin: FDB.Bytes, end: FDB.Bytes) {
        self.subspace.range()
    }

    /// Check if key belongs to this subspace
    ///
    /// - Parameter key: Key to check
    /// - Returns: true if key belongs to this subspace
    public func contains(_ key: Data) -> Bool {
        self.subspace.contains(Array(key))
    }
}

// MARK: - Equatable

extension DirectorySubspace: Equatable {
    public static func == (lhs: DirectorySubspace, rhs: DirectorySubspace) -> Bool {
        lhs.prefix == rhs.prefix &&
        lhs.path == rhs.path &&
        lhs.type == rhs.type
    }
}

// MARK: - Hashable

extension DirectorySubspace: Hashable {
    public func hash(into hasher: inout Hasher) {
        hasher.combine(`prefix`)
        hasher.combine(path)
        hasher.combine(type)
    }
}

// MARK: - CustomStringConvertible

extension DirectorySubspace: CustomStringConvertible {
    public var description: String {
        let pathStr = path.joined(separator: "/")
        if let type = type {
            let layerStr: String
            switch type {
            case .partition:
                layerStr = "partition"
            case .custom(let name):
                layerStr = name
            }
            return "DirectorySubspace(path: \"\(pathStr)\", layer: \"\(layerStr)\")"
        } else {
            return "DirectorySubspace(path: \"\(pathStr)\")"
        }
    }
}

// MARK: - CustomDebugStringConvertible

extension DirectorySubspace: CustomDebugStringConvertible {
    public var debugDescription: String {
        let pathStr = path.joined(separator: "/")
        let prefixHex = subspace.prefix.map { String(format: "%02x", $0) }.joined()
        if let type = type {
            let layerStr: String
            switch type {
            case .partition:
                layerStr = "partition"
            case .custom(let name):
                layerStr = name
            }
            return "DirectorySubspace(path: \"\(pathStr)\", prefix: 0x\(prefixHex), layer: \"\(layerStr)\")"
        } else {
            return "DirectorySubspace(path: \"\(pathStr)\", prefix: 0x\(prefixHex))"
        }
    }
}
