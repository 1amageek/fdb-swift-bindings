import Testing
@testable import FoundationDB

@Suite("Directory subspace")
struct DirectorySubspaceTests {
    @Test("Packed keys preserve the allocated prefix and tuple")
    func packedKeysPreservePrefixAndTuple() throws {
        let prefix = Tuple("directory").pack()
        let directory = DirectorySubspace(
            prefix: prefix,
            path: ["app", "users"],
            type: nil
        )
        let tuple = Tuple("tenant-a", Int64(42))

        let key = directory.pack(tuple)
        let decoded = try directory.unpack(key)

        #expect(directory.prefix == prefix)
        #expect(directory.contains(key))
        #expect(decoded == tuple)
    }

    @Test("Directory metadata remains attached to the allocated prefix")
    func directoryMetadataRemainsAttachedToPrefix() {
        let directory = DirectorySubspace(
            prefix: [0x10, 0x20],
            path: ["tenant", "calendar"],
            type: .partition
        )

        #expect(directory.path == ["tenant", "calendar"])
        #expect(directory.type == .partition)
        #expect(directory.isPartition)
        #expect(directory.subspace.prefix == directory.prefix)
    }
}
