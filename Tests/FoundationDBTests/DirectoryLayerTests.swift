/*
 * DirectoryLayerTests.swift
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

import Testing
@testable import FoundationDB

@Suite("Directory Layer Tests")
struct DirectoryLayerTests {
    let database: any DatabaseProtocol

    init() async throws {
        // Initialize FDB network if needed
        try await FDBClient.maybeInitialize()

        self.database = try FDBClient.openDatabase()

        // Clean up ALL test data ONLY in the test subspace
        // IMPORTANT: Never clear the entire keyspace [0x00, 0xFF]
        // as it would destroy all data in the cluster
        let testSubspace = Subspace(rootPrefix: "directory-layer-tests")

        // Clear the entire test subspace before running any tests
        let (begin, end) = testSubspace.range()
        try await database.withTransaction { transaction in
            transaction.clearRange(
                beginKey: begin,
                endKey: end
            )
        }
    }

    // Helper to create a unique DirectoryLayer for each test
    private func makeDirectoryLayer(name: String) -> DirectoryLayer {
        let testSubspace = Subspace(rootPrefix: "directory-layer-tests").subspace(name)
        return DirectoryLayer(
            database: database,
            nodeSubspace: testSubspace.subspace(0xFE),  // Test metadata
            contentSubspace: testSubspace                // Test data
        )
    }

    // MARK: - Basic Operations

    @Test("Create and open directory")
    func createAndOpen() async throws {
        let directoryLayer = makeDirectoryLayer(name: "createAndOpen")

        // Create directory
        let dir = try await directoryLayer.createOrOpen(path: ["test"])
        #expect(dir.path == ["test"])
        #expect(!dir.prefix.isEmpty)

        // Reopen should return same prefix
        let reopened = try await directoryLayer.open(path: ["test"])
        #expect(reopened.prefix == dir.prefix)
        #expect(reopened.path == ["test"])
    }

    @Test("Create already existing directory throws error")
    func createAlreadyExists() async throws {
        let directoryLayer = makeDirectoryLayer(name: "createAlreadyExists")

        _ = try await directoryLayer.create(path: ["test"])

        // Second create should fail
        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.create(path: ["test"])
        }
    }

    @Test("Open non-existent directory throws error")
    func openNonExistent() async throws {
        let directoryLayer = makeDirectoryLayer(name: "openNonExistent")

        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.open(path: ["nonexistent"])
        }
    }

    @Test("Nested directories")
    func nestedDirectories() async throws {
        let directoryLayer = makeDirectoryLayer(name: "nestedDirectories")

        let parent = try await directoryLayer.createOrOpen(path: ["parent"])
        let child = try await directoryLayer.createOrOpen(path: ["parent", "child"])

        #expect(child.path == ["parent", "child"])
        #expect(parent.prefix != child.prefix)
    }

    // MARK: - Critical Bug Fix: Version Persistence

    @Test("Version persistence across multiple operations")
    func versionPersistence() async throws {
        let directoryLayer = makeDirectoryLayer(name: "versionPersistence")

        // First directory creation writes version
        _ = try await directoryLayer.createOrOpen(path: ["first"])

        // Second operation should not fail (tests version reading bug fix)
        _ = try await directoryLayer.createOrOpen(path: ["second"])

        // Third operation to confirm stability
        let third = try await directoryLayer.open(path: ["second"])
        #expect(third.path == ["second"])
    }

    // MARK: - Partition Operations

    @Test("Partition creation")
    func partitionCreation() async throws {
        let directoryLayer = makeDirectoryLayer(name: "partitionCreation")

        let partition = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1"],
            type: .partition
        )

        #expect(partition.path == ["tenants", "tenant-1"])
        #expect(partition.isPartition)
    }

    @Test("Partition traversal with full path")
    func partitionTraversal() async throws {
        let directoryLayer = makeDirectoryLayer(name: "partitionTraversal")

        // Create partition
        let partition = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1"],
            type: .partition
        )

        // Create subdirectory inside partition using full path
        let orders = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1", "orders"]
        )

        // Verify the full path is preserved
        #expect(orders.path == ["tenants", "tenant-1", "orders"])

        // Verify the prefix is under the partition
        #expect(orders.prefix.starts(with: partition.prefix))

        // Create another subdirectory
        let products = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1", "products"]
        )

        #expect(products.path == ["tenants", "tenant-1", "products"])
        #expect(products.prefix.starts(with: partition.prefix))

        // Verify prefixes are different
        #expect(orders.prefix != products.prefix)
    }

    @Test("Cannot create partition inside partition")
    func cannotCreatePartitionInPartition() async throws {
        let directoryLayer = makeDirectoryLayer(name: "cannotCreatePartitionInPartition")

        // Create first partition
        _ = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1"],
            type: .partition
        )

        // Try to create nested partition
        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.createOrOpen(
                path: ["tenants", "tenant-1", "nested"],
                type: .partition
            )
        }
    }

    // MARK: - Manual Prefix Operations

    @Test("Manual prefix collision detection")
    func manualPrefixCollisionDetection() async throws {
        let directoryLayer = makeDirectoryLayer(name: "manualPrefixCollisionDetection")

        let relativePrefix: FDB.Bytes = [0x01, 0x02, 0x03]

        // First directory with custom prefix (treated as relative)
        let first = try await directoryLayer.create(
            path: ["first"],
            prefix: relativePrefix
        )

        // dir.prefix should be ABSOLUTE (contentSubspace.prefix + relativePrefix)
        let testSubspace = Subspace(rootPrefix: "directory-layer-tests").subspace("manualPrefixCollisionDetection")
        let expectedAbsolutePrefix = testSubspace.prefix + relativePrefix
        #expect(first.prefix == expectedAbsolutePrefix)

        // Try to create second directory with same prefix - should detect collision
        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.create(
                path: ["second"],
                prefix: relativePrefix
            )
        }
    }

    @Test("Manual prefix overlap detection")
    func manualPrefixOverlap() async throws {
        let directoryLayer = makeDirectoryLayer(name: "manualPrefixOverlap")

        let relativePrefix1: FDB.Bytes = [0x01, 0x02]
        let relativePrefix2: FDB.Bytes = [0x01, 0x02, 0x03]  // Overlaps with prefix1

        // First directory
        _ = try await directoryLayer.create(path: ["first"], prefix: relativePrefix1)

        // Try to create with overlapping prefix - should detect collision
        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.create(path: ["second"], prefix: relativePrefix2)
        }
    }

    @Test("Manual prefix in metadata space")
    func manualPrefixInMetadataSpace() async throws {
        let directoryLayer = makeDirectoryLayer(name: "manualPrefixInMetadataSpace")

        // The nodeSubspace for this test uses subspace(0xFE), which is Tuple-encoded
        // subspace(0xFE) adds Tuple(0xFE).encode(), not just [0xFE]
        // We need to find what Tuple(0xFE) actually encodes to

        let testSubspace = Subspace(rootPrefix: "directory-layer-tests").subspace("manualPrefixInMetadataSpace")
        let nodeSubspace = testSubspace.subspace(0xFE)

        // Extract the Tuple-encoded suffix
        let nodeEncoding = Array(nodeSubspace.prefix.dropFirst(testSubspace.prefix.count))

        // Use this as the conflicting relative prefix
        let conflictingRelativePrefix = nodeEncoding

        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.create(path: ["invalid"], prefix: conflictingRelativePrefix)
        }
    }

    // MARK: - Directory Operations

    @Test("List directories")
    func listDirectories() async throws {
        let directoryLayer = makeDirectoryLayer(name: "listDirectories")

        // Create multiple directories
        _ = try await directoryLayer.createOrOpen(path: ["a"])
        _ = try await directoryLayer.createOrOpen(path: ["b"])
        _ = try await directoryLayer.createOrOpen(path: ["c"])

        // List root
        let dirs = try await directoryLayer.list(path: [])
        #expect(Set(dirs) == Set(["a", "b", "c"]))
    }

    @Test("List nested directories")
    func listNestedDirectories() async throws {
        let directoryLayer = makeDirectoryLayer(name: "listNestedDirectories")

        // Create nested structure
        _ = try await directoryLayer.createOrOpen(path: ["parent", "child1"])
        _ = try await directoryLayer.createOrOpen(path: ["parent", "child2"])

        // List parent's children
        let children = try await directoryLayer.list(path: ["parent"])
        #expect(Set(children) == Set(["child1", "child2"]))
    }

    @Test("Remove directory")
    func removeDirectory() async throws {
        let directoryLayer = makeDirectoryLayer(name: "removeDirectory")

        _ = try await directoryLayer.createOrOpen(path: ["test"])

        // Verify exists
        let exists = try await directoryLayer.exists(path: ["test"])
        #expect(exists)

        // Remove
        try await directoryLayer.remove(path: ["test"])

        // Verify removed
        let existsAfter = try await directoryLayer.exists(path: ["test"])
        #expect(!existsAfter)
    }

    @Test("Move directory")
    func moveDirectory() async throws {
        let directoryLayer = makeDirectoryLayer(name: "moveDirectory")

        // Create directory
        let original = try await directoryLayer.createOrOpen(path: ["old"])

        // Move
        let moved = try await directoryLayer.move(
            oldPath: ["old"],
            newPath: ["new"]
        )

        #expect(moved.path == ["new"])
        #expect(moved.prefix == original.prefix)  // Prefix stays the same

        // Verify old path doesn't exist
        let oldExists = try await directoryLayer.exists(path: ["old"])
        #expect(!oldExists)

        // Verify new path exists
        let newExists = try await directoryLayer.exists(path: ["new"])
        #expect(newExists)
    }

    // MARK: - Data Isolation

    @Test("Data isolation between directories")
    func dataIsolation() async throws {
        let directoryLayer = makeDirectoryLayer(name: "dataIsolation")

        let dir1 = try await directoryLayer.createOrOpen(path: ["app1"])
        let dir2 = try await directoryLayer.createOrOpen(path: ["app2"])

        // Write data to each directory
        try await database.withTransaction { transaction in
            let key1 = dir1.pack(Tuple("user:123"))
            let key2 = dir2.pack(Tuple("user:123"))

            transaction.setValue([0x01], for: Array(key1))
            transaction.setValue([0x02], for: Array(key2))
        }

        // Read back and verify isolation
        try await database.withTransaction { transaction in
            let key1 = dir1.pack(Tuple("user:123"))
            let key2 = dir2.pack(Tuple("user:123"))

            let value1 = try await transaction.getValue(for: Array(key1), snapshot: false)
            let value2 = try await transaction.getValue(for: Array(key2), snapshot: false)

            #expect(value1 == [0x01] as FDB.Bytes)
            #expect(value2 == [0x02] as FDB.Bytes)
        }
    }

    // MARK: - Layer Metadata

    @Test("Custom layer")
    func customLayer() async throws {
        let directoryLayer = makeDirectoryLayer(name: "customLayer")

        let custom = try await directoryLayer.createOrOpen(
            path: ["custom"],
            type: .custom("my-layer")
        )

        #expect(custom.type == .custom("my-layer"))

        // Reopen and verify layer
        let reopened = try await directoryLayer.open(path: ["custom"])
        #expect(reopened.type == .custom("my-layer"))
    }

    @Test("Layer mismatch")
    func layerMismatch() async throws {
        let directoryLayer = makeDirectoryLayer(name: "layerMismatch")

        _ = try await directoryLayer.createOrOpen(
            path: ["test"],
            type: .custom("layer1")
        )

        // Try to open with different layer
        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.createOrOpen(
                path: ["test"],
                type: .custom("layer2")
            )
        }
    }

    // MARK: - Cross-Language Compatibility

    @Test("Cross-language metadata structure")
    func crossLanguageMetadataStructure() async throws {
        let directoryLayer = makeDirectoryLayer(name: "crossLanguageMetadataStructure")

        // This test verifies that our metadata structure matches
        // the official Python/Java/Go implementations

        let dir = try await directoryLayer.createOrOpen(path: ["test"])

        // Verify the metadata keys exist with correct structure
        try await database.withTransaction { transaction in
            // nodeSubspace[parentPrefix][subdirs=0][childName] → childPrefix (RELATIVE)
            // Use the actual nodeSubspace for this test
            let testSubspace = Subspace(rootPrefix: "directory-layer-tests").subspace("crossLanguageMetadataStructure")
            let nodeSubspace = testSubspace.subspace(0xFE)

            let subdirKey = nodeSubspace
                .subspace(0)  // subdirs = 0 (root directory's children)
                .pack(Tuple("test"))

            let relativePrefixData = try await transaction.getValue(
                for: Array(subdirKey),
                snapshot: false
            )
            #expect(relativePrefixData != nil)

            // Metadata stores RELATIVE prefix
            // dir.prefix is ABSOLUTE prefix (contentSubspace.prefix + relativePrefix)
            let expectedAbsolutePrefix = testSubspace.prefix + relativePrefixData!
            #expect(Array(dir.prefix) == expectedAbsolutePrefix)
        }
    }

    // MARK: - Bug Detection Tests

    @Test("Deep nested directories stay within partition boundaries")
    func deepNestedDirectoriesStayInPartition() async throws {
        let directoryLayer = makeDirectoryLayer(name: "bugDeepPartitionNesting")

        // Create partition at ["tenants", "tenant-1"]
        let partition = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1"],
            type: .partition
        )

        // Create deeply nested directories inside partition
        _ = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1", "orders"]
        )

        let historyDirectory = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1", "orders", "history"]
        )

        // BUG CHECK 1: All subdirectories should have prefixes under the partition
        #expect(
            historyDirectory.prefix.starts(with: partition.prefix),
            "History directory should be inside partition prefix space"
        )

        // BUG CHECK 2: Verify we can list children correctly (functional test)
        let ordersChildren = try await directoryLayer.list(
            path: ["tenants", "tenant-1", "orders"]
        )
        #expect(
            ordersChildren.contains("history"),
            "Should be able to list 'history' as child of 'orders'"
        )

        // BUG CHECK 3: Verify we can resolve the deep nested directory
        let resolved = try await directoryLayer.open(
            path: ["tenants", "tenant-1", "orders", "history"]
        )
        #expect(resolved.path == ["tenants", "tenant-1", "orders", "history"])
    }

    @Test("Partition subdirectory operations maintain metadata integrity")
    func partitionSubdirectoryOperationsMaintainMetadata() async throws {
        let directoryLayer = makeDirectoryLayer(name: "bugPartitionSubdirectoryOperations")

        // Create partition
        _ = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1"],
            type: .partition
        )

        // Create subdirectory inside partition
        _ = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1", "orders"]
        )

        // BUG CHECK 1: List partition's children should work correctly
        let childrenBeforeNested = try await directoryLayer.list(
            path: ["tenants", "tenant-1"]
        )
        #expect(childrenBeforeNested.contains("orders"), "Should list 'orders' as child")

        // Create nested subdirectory
        _ = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1", "orders", "items"]
        )

        // BUG CHECK 2: List orders' children should work
        let ordersChildren = try await directoryLayer.list(
            path: ["tenants", "tenant-1", "orders"]
        )
        #expect(ordersChildren.contains("items"), "Should list 'items' as child of orders")

        // BUG CHECK 3: Move operation inside partition should work
        let movedDirectory = try await directoryLayer.move(
            oldPath: ["tenants", "tenant-1", "orders", "items"],
            newPath: ["tenants", "tenant-1", "orders", "products"]
        )
        #expect(movedDirectory.path == ["tenants", "tenant-1", "orders", "products"])

        // Verify moved directory is listed correctly
        let ordersChildrenAfterMove = try await directoryLayer.list(
            path: ["tenants", "tenant-1", "orders"]
        )
        #expect(ordersChildrenAfterMove.contains("products"), "Should list 'products' after move")
        #expect(!ordersChildrenAfterMove.contains("items"), "Should not list old name 'items'")

        // BUG CHECK 4: Remove operation inside partition should work
        try await directoryLayer.remove(path: ["tenants", "tenant-1", "orders", "products"])

        let ordersChildrenAfterRemove = try await directoryLayer.list(
            path: ["tenants", "tenant-1", "orders"]
        )
        #expect(ordersChildrenAfterRemove.isEmpty, "Should have no children after removal")
    }

    @Test("Root directory uses content prefix for data writes")
    func rootDirectoryUsesContentPrefix() async throws {
        let directoryLayer = makeDirectoryLayer(name: "bugRootDirectoryPrefix")

        // Get root directory by resolving empty path
        let rootDirectory = try await database.withTransaction { transaction in
            return try await directoryLayer.resolve(transaction: transaction, path: [])
        }

        guard let rootDirectory = rootDirectory else {
            throw DirectoryError.directoryNotFound(path: [])
        }

        // BUG CHECK 1: Root directory prefix should NOT be metadata space (0xFE)
        #expect(
            rootDirectory.prefix != [0xFE],
            "Root directory should not return metadata prefix 0xFE"
        )

        // BUG CHECK 2: Writing data to root directory should not corrupt metadata
        try await database.withTransaction { transaction in
            // Write some data using root directory
            let dataKey = rootDirectory.pack(Tuple("test-data"))
            transaction.setValue([0x01, 0x02, 0x03], for: Array(dataKey))
        }

        // Create a subdirectory after writing to root
        let subDirectory = try await directoryLayer.createOrOpen(path: ["subdir"])

        // BUG CHECK 3: Subdirectory creation should still work (metadata not corrupted)
        #expect(subDirectory.path == ["subdir"])

        // BUG CHECK 4: Verify we can still list root's subdirectories
        // Note: list() with empty path is equivalent to listing root's children
        let children = try await directoryLayer.list()
        #expect(children.contains("subdir"), "Should list subdirectory after root data write")

        // BUG CHECK 5: Read back the data we wrote to root
        let readData = try await database.withTransaction { transaction in
            let dataKey = rootDirectory.pack(Tuple("test-data"))
            return try await transaction.getValue(for: Array(dataKey), snapshot: false)
        }

        #expect(
            readData == [0x01, 0x02, 0x03],
            "Should be able to read data written to root directory"
        )

        // BUG CHECK 6: Verify data didn't leak into metadata space
        try await database.withTransaction { transaction in
            let testSubspace = Subspace(rootPrefix: "directory-layer-tests").subspace("bugRootDirectoryPrefix")
            let nodeSubspace = testSubspace.subspace(0xFE)

            // Check if data corrupted the metadata subspace
            let metadataKey = nodeSubspace.pack(Tuple("test-data"))
            let corruptedMetadata = try await transaction.getValue(
                for: Array(metadataKey),
                snapshot: false
            )

            #expect(
                corruptedMetadata == nil,
                "Data write should NOT corrupt metadata space"
            )
        }
    }

    @Test("Multi-level partition directories support all operations")
    func multiLevelPartitionDirectoryOperations() async throws {
        let directoryLayer = makeDirectoryLayer(name: "bugMultiLevelPartitionTraversal")

        // Create nested structure: app/tenants/tenant-1 (partition)/services/orders/items
        _ = try await directoryLayer.createOrOpen(path: ["app"])
        _ = try await directoryLayer.createOrOpen(path: ["app", "tenants"])

        let partition = try await directoryLayer.createOrOpen(
            path: ["app", "tenants", "tenant-1"],
            type: .partition
        )

        let servicesDirectory = try await directoryLayer.createOrOpen(
            path: ["app", "tenants", "tenant-1", "services"]
        )

        let ordersDirectory = try await directoryLayer.createOrOpen(
            path: ["app", "tenants", "tenant-1", "services", "orders"]
        )

        let itemsDirectory = try await directoryLayer.createOrOpen(
            path: ["app", "tenants", "tenant-1", "services", "orders", "items"]
        )

        // BUG CHECK 1: All directories should be under partition prefix
        #expect(servicesDirectory.prefix.starts(with: partition.prefix))
        #expect(ordersDirectory.prefix.starts(with: partition.prefix))
        #expect(itemsDirectory.prefix.starts(with: partition.prefix))

        // BUG CHECK 2: List at various levels should work
        let tenantChildren = try await directoryLayer.list(
            path: ["app", "tenants", "tenant-1"]
        )
        #expect(tenantChildren.contains("services"))

        let servicesChildren = try await directoryLayer.list(
            path: ["app", "tenants", "tenant-1", "services"]
        )
        #expect(servicesChildren.contains("orders"))

        let ordersChildren = try await directoryLayer.list(
            path: ["app", "tenants", "tenant-1", "services", "orders"]
        )
        #expect(ordersChildren.contains("items"))

        // BUG CHECK 3: Move deep nested directory
        let movedDirectory = try await directoryLayer.move(
            oldPath: ["app", "tenants", "tenant-1", "services", "orders", "items"],
            newPath: ["app", "tenants", "tenant-1", "services", "orders", "products"]
        )
        #expect(movedDirectory.path == ["app", "tenants", "tenant-1", "services", "orders", "products"])

        // BUG CHECK 4: Remove and verify
        try await directoryLayer.remove(
            path: ["app", "tenants", "tenant-1", "services", "orders", "products"]
        )

        let ordersChildrenAfterRemove = try await directoryLayer.list(
            path: ["app", "tenants", "tenant-1", "services", "orders"]
        )
        #expect(ordersChildrenAfterRemove.isEmpty)
    }

    // MARK: - Bug Fix Regression Tests

    @Test("Move preserves prefix correctness on reopen")
    func movePrefixCorrectness() async throws {
        let directoryLayer = makeDirectoryLayer(name: "movePrefixCorrectness")

        // Create a directory
        let original = try await directoryLayer.createOrOpen(path: ["old", "dir"])
        let originalPrefix = original.prefix

        // Write some data
        try await database.withTransaction { transaction in
            let key = original.subspace.pack(Tuple("test"))
            transaction.setValue([0x42], for: Array(key))
        }

        // Move the directory
        let moved = try await directoryLayer.move(
            oldPath: ["old", "dir"],
            newPath: ["new", "dir"]
        )

        // Verify prefix is preserved (move doesn't change prefix)
        #expect(moved.prefix == originalPrefix)

        // Verify reopening returns same prefix
        let reopened = try await directoryLayer.open(path: ["new", "dir"])
        #expect(
            reopened.prefix == originalPrefix,
            "Reopened directory should have same prefix, not doubled"
        )

        // Verify data is still accessible
        let dataExists = try await database.withTransaction { transaction in
            let key = reopened.subspace.pack(Tuple("test"))
            let value = try await transaction.getValue(for: Array(key), snapshot: false)
            return value != nil
        }
        #expect(dataExists, "Data should be accessible after move and reopen")
    }

    @Test("Remove partition root cleans up parent entry")
    func removePartitionRootCleansUpParent() async throws {
        let directoryLayer = makeDirectoryLayer(name: "removePartitionRoot")

        // Create a partition
        _ = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1"],
            type: .partition
        )

        // Create a subdirectory inside the partition
        _ = try await directoryLayer.createOrOpen(
            path: ["tenants", "tenant-1", "data"]
        )

        // List parent before removal
        let tenantsBeforeRemove = try await directoryLayer.list(path: ["tenants"])
        #expect(
            tenantsBeforeRemove.contains("tenant-1"),
            "tenant-1 should exist before removal"
        )

        // Remove the partition root
        try await directoryLayer.remove(path: ["tenants", "tenant-1"])

        // Verify parent no longer lists the removed partition
        let tenantsAfterRemove = try await directoryLayer.list(path: ["tenants"])
        #expect(
            !tenantsAfterRemove.contains("tenant-1"),
            "tenant-1 should be removed from parent's list"
        )

        // Verify opening the removed partition fails
        await #expect(throws: DirectoryError.self) {
            _ = try await directoryLayer.open(path: ["tenants", "tenant-1"])
        }
    }
}
