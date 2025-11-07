/*
 * DirectoryVersion.swift
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

/// Directory Layer version
public struct DirectoryVersion: Sendable, Equatable, Hashable {
    public let major: Int
    public let minor: Int
    public let patch: Int

    /// Current Directory Layer version
    public static let current = DirectoryVersion(major: 1, minor: 0, patch: 0)

    public init(major: Int, minor: Int, patch: Int) {
        self.major = major
        self.minor = minor
        self.patch = patch
    }

    /// Check if this version is compatible with another version
    /// Compatible if major versions match
    public func isCompatible(with other: DirectoryVersion) -> Bool {
        self.major == other.major
    }

    /// Convert to array representation for storage
    var asArray: [Int] {
        [major, minor, patch]
    }

    /// Create from array representation
    init?(from array: [Int]) {
        guard array.count == 3 else { return nil }
        self.major = array[0]
        self.minor = array[1]
        self.patch = array[2]
    }
}
