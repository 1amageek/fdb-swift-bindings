/*
 * DirectoryError.swift
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

/// Errors that can occur during Directory Layer operations
///
/// DirectoryError represents various failure conditions that may occur when
/// working with the FoundationDB Directory Layer.
public enum DirectoryError: Error, Sendable {
    /// Directory not found at the specified path
    ///
    /// Thrown when attempting to open or operate on a directory that doesn't exist.
    /// - Parameter path: The path that was not found
    case directoryNotFound(path: [String])

    /// Directory already exists at the specified path
    ///
    /// Thrown when attempting to create a directory that already exists.
    /// - Parameter path: The path where a directory already exists
    case directoryAlreadyExists(path: [String])

    /// Invalid path (empty or contains empty components)
    ///
    /// Thrown when a path is malformed or contains invalid components.
    /// - Parameters:
    ///   - path: The invalid path
    ///   - reason: Detailed explanation of why the path is invalid
    case invalidPath(path: [String], reason: String)

    /// Layer mismatch between expected and actual
    ///
    /// Thrown when opening a directory with a different layer type than expected.
    /// For example, opening a partition as a normal directory.
    /// - Parameters:
    ///   - expected: The expected layer type
    ///   - actual: The actual layer type found
    case layerMismatch(expected: DirectoryType?, actual: DirectoryType?)

    /// Cannot create partition inside another partition
    ///
    /// Thrown when attempting to create a partition within an existing partition.
    /// Nested partitions are not supported by the Directory Layer specification.
    /// - Parameter path: The path where partition creation was attempted
    case cannotCreatePartitionInPartition(path: [String])

    /// Cannot move directory across partition boundaries
    ///
    /// Thrown when attempting to move a directory from one partition to another.
    /// Directories can only be moved within the same partition.
    /// - Parameters:
    ///   - from: The source path
    ///   - to: The destination path
    case cannotMoveAcrossPartitions(from: [String], to: [String])

    /// Incompatible Directory Layer version
    ///
    /// Thrown when the stored version is incompatible with the current implementation.
    /// - Parameters:
    ///   - stored: The version stored in the database
    ///   - expected: The version expected by this implementation
    case incompatibleVersion(stored: DirectoryVersion, expected: DirectoryVersion)

    /// Invalid version format in stored data
    ///
    /// Thrown when version data in the database is corrupted or malformed.
    /// - Parameter data: The invalid version data
    case invalidVersion(data: Data)

    /// Directory layer not initialized (for partition operations)
    ///
    /// Thrown when attempting partition-specific operations on an uninitialized layer.
    case directoryLayerNotInitialized

    /// Invalid metadata format
    ///
    /// Thrown when directory metadata is corrupted or doesn't match expected format.
    /// - Parameter reason: Detailed explanation of the metadata issue
    case invalidMetadata(reason: String)

    /// Prefix already in use by another directory
    ///
    /// Thrown when attempting to create a directory with a manually-specified prefix
    /// that is already allocated to another directory.
    /// - Parameter prefix: The prefix that is already in use
    case prefixInUse(prefix: FDB.Bytes)

    /// Prefix conflicts with metadata subspace
    ///
    /// Thrown when attempting to use a prefix that would overlap with the
    /// Directory Layer's internal metadata storage (typically starting with 0xFE).
    /// - Parameter prefix: The conflicting prefix
    case prefixInMetadataSpace(prefix: FDB.Bytes)
}

// MARK: - CustomStringConvertible

extension DirectoryError: CustomStringConvertible {
    public var description: String {
        switch self {
        case .directoryNotFound(let path):
            return "Directory not found: \(path.joined(separator: "/"))"

        case .directoryAlreadyExists(let path):
            return "Directory already exists: \(path.joined(separator: "/"))"

        case .invalidPath(let path, let reason):
            return "Invalid path \(path.joined(separator: "/")): \(reason)"

        case .layerMismatch(let expected, let actual):
            let expectedStr = expected?.description ?? "nil"
            let actualStr = actual?.description ?? "nil"
            return "Layer mismatch: expected \(expectedStr), got \(actualStr)"

        case .cannotCreatePartitionInPartition(let path):
            return "Cannot create partition inside another partition: \(path.joined(separator: "/"))"

        case .cannotMoveAcrossPartitions(let from, let to):
            return "Cannot move directory across partitions: \(from.joined(separator: "/")) → \(to.joined(separator: "/"))"

        case .incompatibleVersion(let stored, let expected):
            return "Incompatible Directory Layer version: stored \(stored.major).\(stored.minor).\(stored.patch), expected \(expected.major).\(expected.minor).\(expected.patch)"

        case .invalidVersion(let data):
            return "Invalid version format: \(data.map { String(format: "%02x", $0) }.joined())"

        case .directoryLayerNotInitialized:
            return "Directory layer not initialized (only available for partitions)"

        case .invalidMetadata(let reason):
            return "Invalid metadata: \(reason)"

        case .prefixInUse(let prefix):
            let hexString = prefix.map { String(format: "%02x", $0) }.joined()
            return "Prefix already in use: 0x\(hexString)"

        case .prefixInMetadataSpace(let prefix):
            let hexString = prefix.map { String(format: "%02x", $0) }.joined()
            return "Prefix conflicts with metadata subspace: 0x\(hexString)"
        }
    }
}
