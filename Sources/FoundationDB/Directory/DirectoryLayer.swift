/*
 * DirectoryLayer.swift
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


/// Directory layer type
///
/// Defines the layer type for a directory, which affects how the directory
/// manages its metadata and child directories.
///
/// **Standard Types**:
/// - `partition`: A partition directory that provides isolated namespace
///
/// **Custom Types**:
/// ```swift
/// let customLayer: DirectoryType = .custom("my-layer")
/// ```
public enum DirectoryType: Sendable, Equatable, Hashable {
    /// Standard partition layer type
    ///
    /// Partitions provide isolated namespaces with their own DirectoryLayer instance.
    /// All subdirectories within a partition share a common prefix and cannot be
    /// moved outside the partition boundary.
    case partition

    /// Custom layer type with specified name
    ///
    /// Allows applications to define custom directory layer behaviors.
    /// The name is stored as metadata and can be used to identify special
    /// directory types.
    /// - Parameter name: The custom layer name as a String
    case custom(String)

    /// Raw value as FDB.Bytes for storage
    public var rawValue: FDB.Bytes {
        switch self {
        case .partition:
            return Array("partition".utf8)
        case .custom(let name):
            return Array(name.utf8)
        }
    }

    /// Initialize from raw FDB.Bytes
    ///
    /// - Parameter rawValue: Raw bytes value
    /// - Returns: DirectoryType, or nil if invalid UTF-8
    public init?(rawValue: FDB.Bytes) {
        guard let string = String(bytes: rawValue, encoding: .utf8) else {
            return nil
        }

        switch string {
        case "partition":
            self = .partition
        default:
            self = .custom(string)
        }
    }

    /// String representation of the directory type
    public var description: String {
        switch self {
        case .partition:
            return "partition"
        case .custom(let name):
            return name
        }
    }
}

/// FoundationDB Directory Layer implementation
///
/// This implementation follows the official FoundationDB specification used
/// across Python, Java, Go, and Ruby language bindings.
///
/// **Metadata Structure**:
/// ```
/// nodeSubspace[parentPrefix][subdirs=0][childName] → childPrefix
/// nodeSubspace[nodePrefix]["layer"] → layerBytes
/// nodeSubspace[rootNode]["version"] → versionArray
/// nodeSubspace[rootNode]["hca"] → HCA subspace
/// ```
///
/// **Example**:
/// ```swift
/// let directoryLayer = DirectoryLayer(database: database)
/// let usersDir = try await directoryLayer.createOrOpen(path: ["app", "users"])
/// ```
public final class DirectoryLayer: Sendable {

    // MARK: - Properties

    /// Database instance
    nonisolated(unsafe) private let database: any DatabaseProtocol

    /// Node subspace for directory metadata
    private let nodeSubspace: Subspace

    /// Content subspace for directory data
    private let contentSubspace: Subspace

    /// Root node (same as nodeSubspace for root layer)
    private let rootNode: Subspace

    /// High Contention Allocator for prefix allocation
    private let allocator: HighContentionAllocator

    /// Root directory layer (nil if this IS the root)
    /// This allows DirectorySubspace to always reference the root layer
    /// for consistent absolute path resolution
    private let rootLayer: DirectoryLayer?

    // MARK: - Node Keys

    /// Subdirectory key constant (value = 0)
    private static let subdirs: Int64 = 0

    /// Layer key constant
    private static let layerKey = "layer"

    /// Version key constant
    private static let versionKey = "version"

    // MARK: - Initialization

    /// Initialize Directory Layer with optional custom subspaces
    ///
    /// Creates a new Directory Layer instance for managing hierarchical directory structure.
    /// By default, uses FoundationDB's standard node subspace (prefix 0xFE) for metadata
    /// and an empty prefix for content allocation.
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - nodeSubspace: Node subspace for metadata (default: prefix 0xFE)
    ///   - contentSubspace: Content subspace for data (default: empty prefix)
    ///
    /// **Example**:
    /// ```swift
    /// // Standard Directory Layer
    /// let dir = DirectoryLayer(database: database)
    ///
    /// // Custom subspaces
    /// let customDir = DirectoryLayer(
    ///     database: database,
    ///     nodeSubspace: Subspace(prefix: [0x01, 0xFE]),
    ///     contentSubspace: Subspace(prefix: [0x01])
    /// )
    /// ```
    public convenience init(
        database: any DatabaseProtocol,
        nodeSubspace: Subspace = Subspace(prefix: [0xFE]),
        contentSubspace: Subspace = Subspace(prefix: [])
    ) {
        self.init(
            database: database,
            nodeSubspace: nodeSubspace,
            contentSubspace: contentSubspace,
            rootLayer: nil
        )
    }

    /// Internal designated initializer
    ///
    /// Used internally for creating partition layers with proper parent hierarchy.
    ///
    /// - Parameters:
    ///   - database: Database instance
    ///   - nodeSubspace: Node subspace for metadata
    ///   - contentSubspace: Content subspace for data
    ///   - rootLayer: Root directory layer (for partition hierarchy)
    private init(
        database: any DatabaseProtocol,
        nodeSubspace: Subspace,
        contentSubspace: Subspace,
        rootLayer: DirectoryLayer?
    ) {
        self.database = database
        self.nodeSubspace = nodeSubspace
        self.contentSubspace = contentSubspace
        self.rootNode = nodeSubspace
        self.rootLayer = rootLayer

        // Initialize HCA with nodeSubspace["hca"]
        let hcaSubspace = self.rootNode.subspace("hca")
        self.allocator = HighContentionAllocator(database: database, subspace: hcaSubspace)
    }

    // MARK: - Public Methods

    /// Create or open a directory at the specified path
    ///
    /// - Parameters:
    ///   - path: Directory path as array of strings
    ///   - layer: Optional layer type
    /// - Returns: DirectorySubspace representing the directory
    /// - Throws: DirectoryError if operation fails
    public func createOrOpen(
        path: [String],
        type: DirectoryType? = nil
    ) async throws -> DirectorySubspace {
        try validatePath(path)

        return try await database.withTransaction { transaction in
            try await self.createOrOpenInternal(
                transaction: transaction,
                path: path,
                type: type,
                prefix: nil,
                allowCreate: true,
                allowOpen: true
            )
        }
    }

    /// Create a new directory at the specified path
    ///
    /// - Parameters:
    ///   - path: Directory path as array of strings
    ///   - layer: Optional layer type
    ///   - prefix: Optional custom prefix (if nil, allocates automatically)
    /// - Returns: DirectorySubspace representing the created directory
    /// - Throws: DirectoryError.directoryAlreadyExists if directory exists
    public func create(
        path: [String],
        type: DirectoryType? = nil,
        prefix: FDB.Bytes? = nil
    ) async throws -> DirectorySubspace {
        try validatePath(path)

        return try await database.withTransaction { transaction in
            try await self.createOrOpenInternal(
                transaction: transaction,
                path: path,
                type: type,
                prefix: prefix,
                allowCreate: true,
                allowOpen: false
            )
        }
    }

    /// Open an existing directory at the specified path
    ///
    /// - Parameter path: Directory path as array of strings
    /// - Returns: DirectorySubspace representing the directory
    /// - Throws: DirectoryError.directoryNotFound if directory doesn't exist
    public func open(
        path: [String]
    ) async throws -> DirectorySubspace {
        try validatePath(path)

        return try await database.withTransaction { transaction in
            try await self.createOrOpenInternal(
                transaction: transaction,
                path: path,
                type: nil,
                prefix: nil,
                allowCreate: false,
                allowOpen: true
            )
        }
    }

    /// Move a directory from one path to another
    ///
    /// - Parameters:
    ///   - oldPath: Current directory path
    ///   - newPath: New directory path
    /// - Returns: DirectorySubspace at the new location
    /// - Throws: DirectoryError if source doesn't exist or destination exists
    public func move(
        oldPath: [String],
        newPath: [String]
    ) async throws -> DirectorySubspace {
        try validatePath(oldPath)
        try validatePath(newPath)

        return try await database.withTransaction { transaction in
            // Check partition ancestors for both paths
            let oldPartitionAncestor = try await findPartitionAncestor(
                transaction: transaction,
                path: oldPath
            )
            let newPartitionAncestor = try await findPartitionAncestor(
                transaction: transaction,
                path: newPath
            )

            // Check if both paths are in the same partition - if so, delegate to partition layer
            if let (oldPartitionPath, oldPartitionDir) = oldPartitionAncestor,
               let (newPartitionPath, _) = newPartitionAncestor,
               oldPartitionPath == newPartitionPath {
                // Both paths are in the same partition - delegate to partition layer
                let oldPathInPartition = Array(oldPath.dropFirst(oldPartitionPath.count))
                let newPathInPartition = Array(newPath.dropFirst(newPartitionPath.count))

                let partitionLayer = try createPartitionLayer(prefix: oldPartitionDir.prefix)
                let movedSubspace = try await partitionLayer.move(
                    oldPath: oldPathInPartition,
                    newPath: newPathInPartition
                )

                // Return with absolute path
                // BUG FIX: Same double-prefix issue - partition layer already returns absolute prefix
                return DirectorySubspace(
                    prefix: movedSubspace.prefix,
                    path: newPath,
                    type: movedSubspace.type
                )
            }

            // BUG FIX: Detect cross-partition moves and reject them
            // If partition ancestors don't match, we cannot move across boundaries
            if (oldPartitionAncestor != nil || newPartitionAncestor != nil) &&
               oldPartitionAncestor?.0 != newPartitionAncestor?.0 {
                throw DirectoryError.cannotMoveAcrossPartitions(from: oldPath, to: newPath)
            }

            // Use resolve() for unified path resolution
            guard let oldNode = try await self.resolve(transaction: transaction, path: oldPath) else {
                throw DirectoryError.directoryNotFound(path: oldPath)
            }

            // Check destination doesn't exist
            if let _ = try await self.resolve(transaction: transaction, path: newPath) {
                throw DirectoryError.directoryAlreadyExists(path: newPath)
            }

            // Create parent path if necessary
            if newPath.count > 1 {
                let parentPath = Array(newPath.dropLast())
                _ = try await self.createOrOpenInternal(
                    transaction: transaction,
                    path: parentPath,
                    type: nil,
                    prefix: nil,
                    allowCreate: true,
                    allowOpen: true
                )
            }

            // Remove old parent's reference
            if !oldPath.isEmpty {
                let oldParentPath = Array(oldPath.dropLast())
                let oldDirectoryName = oldPath.last!

                guard let oldParentDirectory = try await self.resolve(transaction: transaction, path: oldParentPath) else {
                    // Parent doesn't exist - this shouldn't happen
                    throw DirectoryError.directoryNotFound(path: oldParentPath)
                }

                // Use unified helper to get metadata
                let oldParentMetadata = try await getMetadata(transaction: transaction, for: oldParentDirectory)
                let oldSubdirectoryKey = oldParentMetadata
                    .subspace(Self.subdirs)
                    .pack(Tuple(oldDirectoryName))
                transaction.clear(key: Array(oldSubdirectoryKey))
            }

            // Add new parent's reference
            let newParentPath = Array(newPath.dropLast())
            let newDirectoryName = newPath.last!

            guard let newParentDirectory = try await self.resolve(transaction: transaction, path: newParentPath) else {
                // Parent doesn't exist - this shouldn't happen after creation above
                throw DirectoryError.directoryNotFound(path: newParentPath)
            }

            // Use unified helper to get metadata
            let newParentMetadata = try await getMetadata(transaction: transaction, for: newParentDirectory)
            let newSubdirectoryKey = newParentMetadata
                .subspace(Self.subdirs)
                .pack(Tuple(newDirectoryName))

            // Convert absolute prefix to relative before storing in metadata.
            // resolve() returns absolute prefix, but metadata stores relative prefix.
            let relativePrefix: FDB.Bytes
            if !contentSubspace.prefix.isEmpty && oldNode.prefix.starts(with: contentSubspace.prefix) {
                relativePrefix = Array(oldNode.prefix.dropFirst(contentSubspace.prefix.count))
            } else {
                relativePrefix = oldNode.prefix
            }

            transaction.setValue(
                Array(relativePrefix),
                for: Array(newSubdirectoryKey)
            )

            // Return DirectorySubspace with new path
            return DirectorySubspace(
                subspace: Subspace(prefix: Array(oldNode.prefix)),
                path: newPath,  // Use original requested newPath
                type: oldNode.type
            )
        }
    }

    /// Move a directory from one location to another (convenience overload).
    ///
    /// This is a convenience method with more fluent argument labels.
    /// It delegates to move(oldPath:newPath:).
    ///
    /// - Parameters:
    ///   - from: Source directory path
    ///   - to: Destination directory path
    /// - Returns: DirectorySubspace at the new location
    /// - Throws: DirectoryError if source doesn't exist or destination exists
    public func move(
        from oldPath: [String],
        to newPath: [String]
    ) async throws -> DirectorySubspace {
        try await move(oldPath: oldPath, newPath: newPath)
    }

    /// Remove a directory and all its contents
    ///
    /// - Parameter path: Directory path to remove
    /// - Throws: DirectoryError.directoryNotFound if directory doesn't exist
    public func remove(
        path: [String]
    ) async throws {
        try validatePath(path)

        try await database.withTransaction { transaction in
            try await self.removeInternal(transaction: transaction, path: path)
        }
    }

    /// Internal remove implementation with transaction parameter
    private func removeInternal(
        transaction: any TransactionProtocol,
        path: [String]
    ) async throws {
        // Check if path is in a partition - if so, delegate to partition layer
        if let (partitionPath, partitionDir) = try await findPartitionAncestor(
            transaction: transaction,
            path: path
        ) {
            // Path is in a partition - delegate to partition layer
            let pathInPartition = Array(path.dropFirst(partitionPath.count))

            let partitionLayer = try createPartitionLayer(prefix: partitionDir.prefix)
            try await partitionLayer.removeInternal(
                transaction: transaction,
                path: pathInPartition
            )

            // When removing the partition root itself (pathInPartition is empty),
            // clean up the parent's reference to this partition.
            // The partition layer cleaned up its own metadata, but cannot access
            // the parent layer's metadata.
            if pathInPartition.isEmpty && !path.isEmpty {
                let parentPath = Array(path.dropLast())
                let directoryName = path.last!

                guard let parentDirectory = try await self.resolve(transaction: transaction, path: parentPath) else {
                    return
                }

                let parentMetadata = try await getMetadata(transaction: transaction, for: parentDirectory)
                let subdirectoryKey = parentMetadata
                    .subspace(Self.subdirs)
                    .pack(Tuple(directoryName))
                transaction.clear(key: Array(subdirectoryKey))
            }

            return
        }

        // Use resolve() for unified path resolution
        guard let node = try await self.resolve(transaction: transaction, path: path) else {
            throw DirectoryError.directoryNotFound(path: path)
        }

        // Remove recursively
        try await removeRecursive(transaction: transaction, dir: node)

        // Remove parent's reference
        if !node.path.isEmpty {
            let parentPath = Array(node.path.dropLast())
            let directoryName = node.path.last!

            guard let parentDirectory = try await self.resolve(transaction: transaction, path: parentPath) else {
                // Parent doesn't exist - this shouldn't happen but handle gracefully
                return
            }

            // Calculate parent metadata using unified helper
            let parentMetadata = try await getMetadata(transaction: transaction, for: parentDirectory)
            let subdirectoryKey = parentMetadata
                .subspace(Self.subdirs)
                .pack(Tuple(directoryName))
            transaction.clear(key: Array(subdirectoryKey))
        }
    }

    /// Check if a directory exists
    ///
    /// - Parameter path: Directory path to check
    /// - Returns: true if directory exists, false otherwise
    public func exists(
        path: [String]
    ) async throws -> Bool {
        try validatePath(path)

        return try await database.withTransaction { transaction in
            let node = try await self.resolve(transaction: transaction, path: path)
            return node != nil
        }
    }

    /// List subdirectories of a directory
    ///
    /// - Parameter path: Directory path to list
    /// - Returns: Array of subdirectory names
    /// - Throws: DirectoryError.directoryNotFound if directory doesn't exist
    public func list(
        path: [String]
    ) async throws -> [String] {
        return try await database.withTransaction { transaction in
            // Check if path is inside a partition - if so, delegate to partition layer
            if let (partitionPath, partitionDirectory) = try await findPartitionAncestor(
                transaction: transaction,
                path: path
            ) {
                // Path is inside a partition - delegate to partition layer
                let pathInPartition = Array(path.dropFirst(partitionPath.count))
                let partitionLayer = try createPartitionLayer(prefix: partitionDirectory.prefix)
                return try await partitionLayer.list(path: pathInPartition)
            }

            // Normal path - use resolve() for unified path resolution
            guard let directory = try await self.resolve(transaction: transaction, path: path) else {
                throw DirectoryError.directoryNotFound(path: path)
            }

            // Use unified helper that works for both partition and normal directories
            return try await self.listChildren(
                transaction: transaction,
                directory: directory
            )
        }
    }

    // MARK: - Internal Operations

    /// Internal create or open implementation
    ///
    /// Swift design: Clean separation between path resolution and directory creation.
    /// All paths are absolute from root.
    private func createOrOpenInternal(
        transaction: any TransactionProtocol,
        path: [String],
        type: DirectoryType?,
        prefix: FDB.Bytes?,
        allowCreate: Bool,
        allowOpen: Bool
    ) async throws -> DirectorySubspace {
        // Check version on first access
        try await checkVersion(transaction: transaction)

        // Resolve the path
        if let existingNode = try await self.resolve(transaction: transaction, path: path) {
            // === Handle existing directory ===
            guard allowOpen else {
                throw DirectoryError.directoryAlreadyExists(path: path)
            }

            // Check layer compatibility
            if let expectedLayer = type, existingNode.type != expectedLayer {
                throw DirectoryError.layerMismatch(expected: expectedLayer, actual: existingNode.type)
            }

            // Return the existing directory
            return existingNode
        }

        // === Handle new directory ===
        guard allowCreate else {
            throw DirectoryError.directoryNotFound(path: path)
        }

        // Check if any ancestor is a partition - if so, delegate to partition layer
        if let (partitionPath, partitionDirectory) = try await findPartitionAncestor(
            transaction: transaction,
            path: path
        ) {
            // Found a partition ancestor - delegate creation to partition layer
            let pathInPartition = Array(path.dropFirst(partitionPath.count))
            let partitionLayer = try createPartitionLayer(prefix: partitionDirectory.prefix)

            let subspace = try await partitionLayer.createOrOpenInternal(
                transaction: transaction,
                path: pathInPartition,
                type: type,
                prefix: prefix,
                allowCreate: allowCreate,
                allowOpen: allowOpen
            )

            // Return with absolute path and absolute prefix
            // BUG FIX: partitionLayer already returns absolute prefix (contentSubspace.prefix + newPrefix)
            // Don't prepend partition prefix again - it causes double-prefixing
            return DirectorySubspace(
                prefix: subspace.prefix,
                path: path,
                type: subspace.type
            )
        }

        // No partition ancestors - proceed with normal creation in this layer
        let parentPath = Array(path.dropLast())
        if !parentPath.isEmpty {
            // Ensure parent exists (may need to create it)
            if let _ = try await resolve(transaction: transaction, path: parentPath) {
                // Parent exists, continue below
            } else {
                // Recursively create parent
                _ = try await createOrOpenInternal(
                    transaction: transaction,
                    path: parentPath,
                    type: nil,
                    prefix: nil,
                    allowCreate: true,
                    allowOpen: true
                )
            }
        }

        // Check partition nesting - partitions cannot be created inside other partitions
        // If rootLayer is not nil, this layer is already inside a partition
        if type == .partition && rootLayer != nil {
            throw DirectoryError.cannotCreatePartitionInPartition(path: path)
        }

        // Allocate prefix
        // Determine the prefix to use
        let absolutePrefix: FDB.Bytes
        let relativePrefix: FDB.Bytes

        if let manualPrefix = prefix {
            // Manual prefix is treated as RELATIVE (same coordinate system as HCA)
            relativePrefix = manualPrefix
            absolutePrefix = contentSubspace.prefix + relativePrefix

            // Validate the absolute prefix
            try await validatePrefix(transaction: transaction, prefix: absolutePrefix)
        } else {
            // HCA allocation - returns relative prefix
            relativePrefix = try await self.allocator.allocate(transaction: transaction)
            absolutePrefix = contentSubspace.prefix + relativePrefix
        }

        // Store subdirectory reference in parent
        guard let dirName = path.last else {
            throw DirectoryError.invalidPath(path: path, reason: "Cannot create directory with empty name")
        }

        // Get parent's metadata subspace
        let parentMetadata: Subspace
        if parentPath.isEmpty {
            // Root directory - use this layer's root node
            parentMetadata = self.rootNode
        } else {
            // Parent exists in this layer (we checked/created it above)
            // Calculate parent metadata
            guard let freshParentDirectory = try await resolve(
                transaction: transaction,
                path: parentPath
            ) else {
                throw DirectoryError.directoryNotFound(path: parentPath)
            }

            // Use getMetadata() helper for consistency
            parentMetadata = try await getMetadata(transaction: transaction, for: freshParentDirectory)
        }

        // Store RELATIVE prefix in metadata
        let subdirKey = parentMetadata
            .subspace(Self.subdirs)
            .pack(Tuple(dirName))
        transaction.setValue(Array(relativePrefix), for: Array(subdirKey))

        // Store layer metadata using relative prefix
        if let layerType = type {
            let prefixSubspace = Subspace(prefix: self.nodeSubspace.prefix + relativePrefix)
            let layerKey = prefixSubspace.pack(Tuple(Self.layerKey))
            transaction.setValue(layerType.rawValue, for: Array(layerKey))
        }

        // Create and return DirectorySubspace
        if type == .partition {
            // For partitions, absolutePrefix becomes the partition's contentSubspace
            let partitionLayer = try createPartitionLayer(prefix: absolutePrefix)
            return DirectorySubspace(
                subspace: partitionLayer.contentSubspace,
                path: path,
                type: type
            )
        }

        // Normal directory - return with ABSOLUTE prefix
        return DirectorySubspace(
            subspace: Subspace(prefix: Array(absolutePrefix)),
            path: path,
            type: type
        )
    }

    /// Resolve path to DirectorySubspace
    ///
    /// Swift design: Simple, unified path resolution with automatic partition traversal.
    /// Always operates on absolute paths from root.
    ///
    /// - Parameters:
    ///   - transaction: Transaction to use for lookups
    ///   - path: Absolute path to resolve
    /// - Returns: DirectorySubspace if exists, nil otherwise
    internal func resolve(
        transaction: any TransactionProtocol,
        path: [String]
    ) async throws -> DirectorySubspace? {
        let currentLayer = self
        var currentPath: [String] = []
        var currentPrefix = rootNode.prefix
        var currentMetadata = rootNode

        // Empty path = root directory
        if path.isEmpty {
            // Root directory should return contentSubspace.prefix for data writes
            // This ensures data is written to the content space, not metadata space
            return DirectorySubspace(
                prefix: contentSubspace.prefix,
                path: [],
                type: nil
            )
        }

        // Traverse path one component at a time
        for (index, name) in path.enumerated() {
            currentPath.append(name)

            // Look up subdirectory in current layer
            let subdirKey = currentMetadata
                .subspace(Self.subdirs)
                .pack(Tuple(name))

            guard let relativePrefix = try await transaction.getValue(
                for: Array(subdirKey),
                snapshot: false
            ) else {
                // Directory doesn't exist
                return nil
            }

            // Directory exists - convert RELATIVE prefix to ABSOLUTE
            let absolutePrefix = contentSubspace.prefix + relativePrefix
            currentPrefix = absolutePrefix
            currentMetadata = Subspace(prefix: currentLayer.nodeSubspace.prefix + relativePrefix)
            let type = try await loadLayer(transaction: transaction, subspace: currentMetadata)

            // Check if this is a partition
            if type == .partition {
                // Get remaining path components
                let remaining = Array(path.dropFirst(index + 1))

                if remaining.isEmpty {
                    // This IS the partition node itself - return with absolute prefix
                    return DirectorySubspace(
                        prefix: absolutePrefix,
                        path: currentPath,
                        type: type
                    )
                }

                // Traverse into partition for remaining path
                let partitionLayer = try createPartitionLayer(prefix: absolutePrefix)
                guard let partitionDir = try await partitionLayer.resolve(
                    transaction: transaction,
                    path: remaining
                ) else {
                    return nil
                }

                // Return with absolute path and absolute prefix
                // partitionDir.prefix is already absolute from partition layer
                return DirectorySubspace(
                    prefix: partitionDir.prefix,
                    path: currentPath + partitionDir.path,
                    type: partitionDir.type
                )
            }

            // Normal directory - continue to next component
        }

        // Reached end of path - return with ABSOLUTE prefix
        return DirectorySubspace(
            prefix: currentPrefix,
            path: currentPath,
            type: try await loadLayer(transaction: transaction, subspace: currentMetadata)
        )
    }

    /// Legacy find method (calls resolve and converts to Node)
    private func find(
        transaction: any TransactionProtocol,
        path: [String]
    ) async throws -> Node {
        let dir = try await resolve(transaction: transaction, path: path)
        if let dir = dir {
            // Directory exists
            let metadata = Subspace(prefix: nodeSubspace.prefix + dir.prefix)
            return Node(
                subspace: metadata,
                path: dir.path,
                prefix: dir.prefix,
                exists: true,
                type: dir.type
            )
        } else {
            // Directory doesn't exist
            return Node(
                subspace: nodeSubspace,
                path: path,
                prefix: [],
                exists: false,
                type: nil
            )
        }
    }

    /// Load layer metadata for a node
    private func loadLayer(
        transaction: any TransactionProtocol,
        subspace: Subspace
    ) async throws -> DirectoryType? {
        let layerKey = subspace.pack(Tuple(Self.layerKey))
        guard let layerData = try await transaction.getValue(for: Array(layerKey), snapshot: false) else {
            return nil
        }
        return DirectoryType(rawValue: layerData)
    }

    /// List subdirectories of a node
    private func listInternal(
        transaction: any TransactionProtocol,
        node: Subspace
    ) async throws -> [String] {
        let subdirsSubspace = node.subspace(Self.subdirs)
        let (begin, end) = subdirsSubspace.range()

        var children: Set<String> = []

        let sequence = transaction.getRange(
            beginSelector: .firstGreaterOrEqual(begin),
            endSelector: .firstGreaterOrEqual(end),
            snapshot: true
        )

        for try await (key, _) in sequence {
            do {
                let tuple = try subdirsSubspace.unpack(Array(key))
                if let childName = tuple[0] as? String {
                    children.insert(childName)
                }
            } catch {
                // Skip keys that don't belong to this subspace
                // This can happen during partition traversal or with corrupted data
                continue
            }
        }

        return Array(children).sorted()
    }

    // MARK: - Partition Helper Methods

    /// Find the closest partition ancestor in a path
    ///
    /// Traverses the path from root to target, returning the first partition encountered.
    ///
    /// - Parameters:
    ///   - transaction: Transaction to use
    ///   - path: Path to check for partition ancestors
    /// - Returns: Tuple of (partition path, partition directory) if found, nil otherwise
    private func findPartitionAncestor(
        transaction: any TransactionProtocol,
        path: [String]
    ) async throws -> (path: [String], directory: DirectorySubspace)? {
        // Empty path has no ancestors
        guard !path.isEmpty else {
            return nil
        }

        // Check each ancestor from immediate parent down to root
        for depth in (1...path.count).reversed() {
            let ancestorPath = Array(path.prefix(depth))

            if let ancestor = try await resolve(transaction: transaction, path: ancestorPath) {
                if ancestor.type == .partition {
                    return (ancestorPath, ancestor)
                }
            }
        }

        return nil
    }

    // MARK: - Unified Helper Methods

    /// Calculate metadata subspace for a directory
    ///
    /// Metadata is stored at: nodeSubspace.prefix + relativePrefix
    /// where relativePrefix = directory.prefix - contentSubspace.prefix
    ///
    /// - Parameters:
    ///   - transaction: Transaction (unused but kept for API compatibility)
    ///   - directory: Directory to get metadata for
    /// - Returns: Subspace containing the directory's metadata
    private func getMetadata(
        transaction: any TransactionProtocol,
        for directory: DirectorySubspace
    ) async throws -> Subspace {
        // Special case: root directory uses rootNode directly
        if directory.path.isEmpty {
            return rootNode
        }

        // directory.prefix is always ABSOLUTE
        // Convert to RELATIVE prefix by stripping contentSubspace.prefix
        let relativePrefix: FDB.Bytes
        if !contentSubspace.prefix.isEmpty && directory.prefix.starts(with: contentSubspace.prefix) {
            relativePrefix = Array(directory.prefix.dropFirst(contentSubspace.prefix.count))
        } else {
            relativePrefix = directory.prefix
        }

        // Metadata subspace = nodeSubspace.prefix + relativePrefix
        return Subspace(prefix: nodeSubspace.prefix + relativePrefix)
    }

    /// List children of a directory (unified for partition and normal directories)
    ///
    /// This method handles both partition and normal directories transparently:
    /// - For partitions: Delegates to the partition's DirectoryLayer
    /// - For normal directories: Lists subdirectories from metadata
    ///
    /// - Parameters:
    ///   - transaction: Transaction to use
    ///   - directory: Directory to list children of
    /// - Returns: Array of child directory names
    private func listChildren(
        transaction: any TransactionProtocol,
        directory: DirectorySubspace
    ) async throws -> [String] {
        // Handle partition: delegate to partition layer
        if directory.type == .partition {
            let partitionLayer = try createPartitionLayer(prefix: directory.prefix)
            return try await partitionLayer.listInternal(
                transaction: transaction,
                node: partitionLayer.rootNode
            )
        }

        // Handle normal directory: list from metadata
        let directoryMetadata = try await getMetadata(transaction: transaction, for: directory)
        return try await listInternal(
            transaction: transaction,
            node: directoryMetadata
        )
    }

    /// Remove DirectorySubspace and all its descendants recursively
    ///
    /// This method uses unified helpers to work correctly for both partition
    /// and normal directories.
    ///
    /// - Parameters:
    ///   - transaction: Transaction to use
    ///   - directory: Directory to remove recursively
    private func removeRecursive(
        transaction: any TransactionProtocol,
        dir directory: DirectorySubspace
    ) async throws {
        // List ALL children using unified helper (works for partition and normal)
        let childNames = try await listChildren(
            transaction: transaction,
            directory: directory
        )

        // Recursively remove children
        for childName in childNames {
            guard let childDirectory = try await self.resolve(
                transaction: transaction,
                path: directory.path + [childName]
            ) else {
                continue
            }
            try await self.removeRecursive(
                transaction: transaction,
                dir: childDirectory
            )
        }

        // Remove directory metadata using unified helper
        let directoryMetadata = try await getMetadata(transaction: transaction, for: directory)
        let metadataRange = directoryMetadata.range()
        transaction.clearRange(
            beginKey: Array(metadataRange.begin),
            endKey: Array(metadataRange.end)
        )

        // Remove directory content
        let contentSubspace = Subspace(prefix: directory.prefix)
        let contentRange = contentSubspace.range()
        transaction.clearRange(
            beginKey: Array(contentRange.begin),
            endKey: Array(contentRange.end)
        )
    }

    /// Create partition layer from node
    private func createPartitionLayer(from node: Node) throws -> DirectoryLayer {
        return try createPartitionLayer(prefix: node.prefix)
    }

    /// Create partition layer with prefix
    private func createPartitionLayer(prefix: FDB.Bytes) throws -> DirectoryLayer {
        // Partition's nodeSubspace is at prefix + 0xFE (raw bytes, not tuple-encoded)
        // This matches the official specification where 0xFE is a reserved system prefix
        let partitionNodeSubspace = Subspace(prefix: prefix + [0xFE])
        let partitionContentSubspace = Subspace(prefix: prefix)

        // DESIGN FIX: Propagate rootLayer
        // If this layer is already inside a partition, pass through its rootLayer
        // Otherwise, this layer IS the root, so pass self as the root
        return DirectoryLayer(
            database: database,
            nodeSubspace: partitionNodeSubspace,
            contentSubspace: partitionContentSubspace,
            rootLayer: self.rootLayer ?? self  // Propagate root layer
        )
    }

    /// Check and initialize version
    private func checkVersion(transaction: any TransactionProtocol) async throws {
        let versionKey = rootNode.pack(Tuple(Self.versionKey))

        if let versionData = try await transaction.getValue(for: Array(versionKey), snapshot: false) {
            // Parse stored version
            let tuple = try Tuple.unpack(from: versionData)
            guard tuple.count == 3 else {
                throw DirectoryError.invalidVersion(data: Data(versionData))
            }

            // Try to extract version numbers (could be Int or Int64)
            func extractInt(_ element: any TupleElement) -> Int? {
                if let value = element as? Int {
                    return value
                } else if let value = element as? Int64 {
                    return Int(value)
                } else if let value = element as? Int32 {
                    return Int(value)
                }
                return nil
            }

            guard let major = extractInt(tuple[0]),
                  let minor = extractInt(tuple[1]),
                  let patch = extractInt(tuple[2]) else {
                throw DirectoryError.invalidVersion(data: Data(versionData))
            }

            let storedVersion = DirectoryVersion(
                major: major,
                minor: minor,
                patch: patch
            )

            // Check compatibility
            if !DirectoryVersion.current.isCompatible(with: storedVersion) {
                throw DirectoryError.incompatibleVersion(
                    stored: storedVersion,
                    expected: DirectoryVersion.current
                )
            }
        } else {
            // First time - write version
            let version = DirectoryVersion.current
            let versionTuple = Tuple(version.major, version.minor, version.patch)
            transaction.setValue(
                versionTuple.pack(),
                for: Array(versionKey)
            )
        }
    }

    /// Validate path
    private func validatePath(_ path: [String]) throws {
        guard !path.isEmpty else {
            throw DirectoryError.invalidPath(path: path, reason: "Path cannot be empty")
        }

        for component in path {
            guard !component.isEmpty else {
                throw DirectoryError.invalidPath(path: path, reason: "Path component cannot be empty")
            }
        }
    }

    /// Validate user-supplied prefix for collisions
    ///
    /// Checks if the prefix conflicts with:
    /// 1. System metadata subspaces (0xFE)
    /// 2. Prefixes already allocated by HCA
    /// 3. Prefixes stored in nodeSubspace subdirectories
    ///
    /// - Parameters:
    ///   - transaction: Transaction to use for validation
    ///   - prefix: User-supplied prefix to validate
    /// - Throws: DirectoryError if prefix conflicts with metadata or existing directories
    private func validatePrefix(
        transaction: any TransactionProtocol,
        prefix: FDB.Bytes
    ) async throws {
        // 1. Check if prefix conflicts with metadata subspaces
        if isPrefixInMetadataSpace(prefix) {
            throw DirectoryError.prefixInMetadataSpace(prefix: prefix)
        }

        // 2. Check HCA recent allocations
        // HCA allocates prefixes as Tuple(candidate).encode() where candidate is an Int64
        // So if this prefix is HCA-allocated, we need to decode it back to the candidate
        // and check recent.pack(Tuple(candidate))
        let hcaSubspace = rootNode.subspace("hca")
        let recentSubspace = hcaSubspace.subspace(1)  // HCA recent subspace

        // Try to decode prefix as a tuple to get the candidate number
        // If it's not HCA-allocated, this will fail or return non-integer
        do {
            let elements = try Tuple.unpack(from: prefix)
            // If prefix decodes to a single integer, it might be HCA-allocated
            if elements.count == 1, let candidate = elements[0] as? Int64 {
                // Check if this candidate is in recent allocations
                let recentKey = recentSubspace.pack(Tuple(candidate))
                if let _ = try await transaction.getValue(for: Array(recentKey), snapshot: false) {
                    throw DirectoryError.prefixInUse(prefix: prefix)
                }
            }
        } catch {
            // Prefix is not tuple-encoded (manual prefix), skip HCA check
            // Manual prefixes won't be in HCA recent allocations anyway
        }

        // 3. Scan subdirectory entries only (not all metadata)
        // We need to check all parent[SUBDIRS][*] = prefix entries
        // This is more targeted than scanning the entire nodeSubspace
        try await validatePrefixRecursive(
            transaction: transaction,
            prefix: prefix,
            node: rootNode,
            path: []
        )
    }

    /// Recursively validate prefix against directory tree
    ///
    /// - Parameters:
    ///   - transaction: Transaction to use
    ///   - prefix: ABSOLUTE prefix to validate
    ///   - node: Current node subspace
    ///   - path: Current path
    private func validatePrefixRecursive(
        transaction: any TransactionProtocol,
        prefix: FDB.Bytes,
        node: Subspace,
        path: [String]
    ) async throws {
        // Check subdirectories at this level
        let subdirsSubspace = node.subspace(Self.subdirs)
        let (begin, end) = subdirsSubspace.range()

        for try await (key, value) in transaction.getRange(
            beginKey: begin,
            endKey: end,
            snapshot: true  // Use snapshot for read-only validation
        ) {
            // value is the RELATIVE prefix of a subdirectory
            let relativePrefix = value

            // Convert to ABSOLUTE prefix for comparison
            let existingAbsolutePrefix = contentSubspace.prefix + relativePrefix

            // Check for conflicts with absolute prefixes
            if existingAbsolutePrefix == prefix {
                throw DirectoryError.prefixInUse(prefix: prefix)
            }

            // Check if one prefix is a prefix of the other
            if prefix.starts(with: existingAbsolutePrefix) || existingAbsolutePrefix.starts(with: prefix) {
                throw DirectoryError.prefixInUse(prefix: prefix)
            }

            // IMPORTANT: Recursively check this subdirectory's children
            // We must check ALL levels, not just direct subdirs
            // Extract child name from key: node[SUBDIRS][childName]
            do {
                let keyTuple = try subdirsSubspace.unpack(key)
                if keyTuple.count > 0, let childName = keyTuple[0] as? String {
                    // Create child node subspace - relativePrefix is raw bytes
                    let childNode = Subspace(prefix: nodeSubspace.prefix + relativePrefix)
                    let childPath = path + [childName]

                    // Check if this child is a partition
                    let layer = try await loadLayer(transaction: transaction, subspace: childNode)

                    if layer == .partition {
                        // Delegate validation to the partition's internal DirectoryLayer
                        // Only validate if the candidate prefix is inside the partition
                        if prefix.starts(with: existingAbsolutePrefix) {
                            // Convert absolute prefix to partition's coordinate system
                            let partitionRelativePrefix = Array(prefix.dropFirst(existingAbsolutePrefix.count))
                            let partitionLayer = try createPartitionLayer(prefix: existingAbsolutePrefix)
                            try await partitionLayer.validatePrefixRecursive(
                                transaction: transaction,
                                prefix: partitionLayer.contentSubspace.prefix + partitionRelativePrefix,
                                node: partitionLayer.rootNode,
                                path: childPath
                            )
                        }
                        // If prefix doesn't start with partition prefix, it won't conflict with partition contents
                    } else {
                        // Recurse into child directory normally
                        try await validatePrefixRecursive(
                            transaction: transaction,
                            prefix: prefix,
                            node: childNode,
                            path: childPath
                        )
                    }
                }
            } catch {
                // Skip keys that can't be unpacked (might be from different subspace)
                continue
            }
        }
    }

    /// Check if prefix conflicts with metadata subspace
    ///
    /// Prefixes should be in contentSubspace but NOT in nodeSubspace.
    /// Only check for conflicts with nodeSubspace (metadata space).
    private func isPrefixInMetadataSpace(_ prefix: FDB.Bytes) -> Bool {
        // Check if prefix overlaps with nodeSubspace (metadata space)
        // This is the only conflict we care about
        if prefix.starts(with: nodeSubspace.prefix) ||
           nodeSubspace.prefix.starts(with: prefix) {
            return true
        }

        return false
    }

    /// Create or open directory with default layer
    public func createOrOpen(path: [String]) async throws -> DirectorySubspace {
        try await createOrOpen(path: path, type: nil)
    }

    /// Create directory with default layer and prefix
    public func create(path: [String]) async throws -> DirectorySubspace {
        try await create(path: path, type: nil, prefix: nil)
    }

    /// Create directory with default prefix
    public func create(path: [String], type: DirectoryType?) async throws -> DirectorySubspace {
        try await create(path: path, type: type, prefix: nil)
    }

    /// List root directories
    public func list() async throws -> [String] {
        try await list(path: [])
    }

    // MARK: - Node

    /// Internal representation of a directory node
    private struct Node {
        let subspace: Subspace
        let path: [String]
        let prefix: FDB.Bytes
        let exists: Bool
        let type: DirectoryType?
    }
}

// MARK: - Database Extension

extension DatabaseProtocol {
    /// Create a Directory Layer instance
    ///
    /// Creates a new Directory Layer instance for managing hierarchical directory structure.
    /// By default, uses FoundationDB's standard configuration (node subspace prefix 0xFE,
    /// empty content subspace prefix).
    ///
    /// **Note**: This creates a new instance on every call.
    /// For better performance, create once and reuse:
    ///
    /// ```swift
    /// let directoryLayer = database.makeDirectoryLayer()
    /// // Reuse directoryLayer for multiple operations
    /// ```
    ///
    /// - Parameters:
    ///   - nodeSubspace: Node subspace for metadata (default: prefix 0xFE)
    ///   - contentSubspace: Content subspace for data (default: empty prefix)
    /// - Returns: New DirectoryLayer instance
    public func makeDirectoryLayer(
        nodeSubspace: Subspace = Subspace(prefix: [0xFE]),
        contentSubspace: Subspace = Subspace(prefix: [])
    ) -> DirectoryLayer {
        DirectoryLayer(
            database: self,
            nodeSubspace: nodeSubspace,
            contentSubspace: contentSubspace
        )
    }
}
