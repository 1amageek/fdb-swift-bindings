// SchemaFileParserTests.swift
// Tests for YAML schema parsing

import Testing
import Foundation
@testable import DatabaseCLICore
@testable import DatabaseEngine
import Core

@Suite("Schema File Parser Tests")
struct SchemaFileParserTests {

    @Test("Parse simple schema")
    func testParseSimpleSchema() throws {
        let yaml = """
        User:
          #Directory: [app, users]

          id: string
          name: string
          age: int64
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.typeName == "User")
        #expect(catalog.fields.count == 3)
        #expect(catalog.directoryComponents.count == 2)
        #expect(catalog.indexes.isEmpty)

        #expect(catalog.fields[0].name == "id")
        #expect(catalog.fields[0].type == .string)
        #expect(catalog.fields[1].name == "name")
        #expect(catalog.fields[2].name == "age")
        #expect(catalog.fields[2].type == .int64)
    }

    @Test("Parse schema with scalar index")
    func testParseScalarIndex() throws {
        let yaml = """
        User:
          #Directory: [app, users]

          id: string
          email: string#scalar(unique:true)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        let index = catalog.indexes[0]
        #expect(index.kindIdentifier == "scalar")
        #expect(index.fieldNames == ["email"])
        #expect(index.unique == true)
    }

    @Test("Parse schema with vector index")
    func testParseVectorIndex() throws {
        let yaml = """
        Product:
          #Directory: [catalog]

          id: string
          embedding: array<double>#vector(dimensions:384, metric:cosine, algorithm:hnsw)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        let index = catalog.indexes[0]
        #expect(index.kindIdentifier == "vector")
        #expect(index.metadata["dimensions"] == "384")
        #expect(index.metadata["metric"] == "cosine")
        #expect(index.metadata["algorithm"] == "hnsw")
    }

    @Test("Parse schema with fulltext index")
    func testParseFullTextIndex() throws {
        let yaml = """
        Article:
          #Directory: [content]

          id: string
          title: string#fulltext(language:english, tokenizer:standard)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        let index = catalog.indexes[0]
        #expect(index.kindIdentifier == "fulltext")
        #expect(index.metadata["language"] == "english")
        #expect(index.metadata["tokenizer"] == "standard")
    }

    @Test("Parse schema with spatial index")
    func testParseSpatialIndex() throws {
        let yaml = """
        Location:
          #Directory: [geo]

          id: string
          coordinates: array<double>#spatial(strategy:geohash)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        let index = catalog.indexes[0]
        #expect(index.kindIdentifier == "spatial")
        #expect(index.metadata["strategy"] == "geohash")
    }

    @Test("Parse schema with rank index")
    func testParseRankIndex() throws {
        let yaml = """
        Document:
          #Directory: [docs]

          id: string
          rank: double#rank
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        #expect(catalog.indexes[0].kindIdentifier == "rank")
    }

    @Test("Parse schema with bitmap index")
    func testParseBitmapIndex() throws {
        let yaml = """
        User:
          #Directory: [app]

          id: string
          status: string#bitmap
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        #expect(catalog.indexes[0].kindIdentifier == "bitmap")
    }

    @Test("Parse schema with leaderboard index")
    func testParseLeaderboardIndex() throws {
        let yaml = """
        Player:
          #Directory: [game]

          id: string
          score: int64#leaderboard(name:global_ranking)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        let index = catalog.indexes[0]
        #expect(index.kindIdentifier == "leaderboard")
        #expect(index.metadata["leaderboardName"] == "global_ranking")
    }

    @Test("Parse schema with aggregation index")
    func testParseAggregationIndex() throws {
        let yaml = """
        Order:
          #Directory: [orders]

          id: string
          total: double#aggregation(functions:sum,count,avg)
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        let index = catalog.indexes[0]
        #expect(index.kindIdentifier == "aggregation")
        #expect(index.metadata["functions"] == "sum,count,avg")
    }

    @Test("Parse schema with version index")
    func testParseVersionIndex() throws {
        let yaml = """
        Document:
          #Directory: [docs]

          id: string
          version: int64#version
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        #expect(catalog.indexes[0].kindIdentifier == "version")
    }

    @Test("Parse schema with graph index")
    func testParseGraphIndex() throws {
        let yaml = """
        Follow:
          #Directory: [social, follows]

          id: string
          follower: string
          following: string

          #Index:
            - kind: graph
              name: social_graph
              from: follower
              edge: follows
              to: following
              strategy: tripleStore
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.indexes.count == 1)
        let index = catalog.indexes[0]
        #expect(index.kindIdentifier == "graph")
        #expect(index.metadata["fromField"] == "follower")
        #expect(index.metadata["edgeField"] == "follows")
        #expect(index.metadata["toField"] == "following")
        #expect(index.metadata["strategy"] == "tripleStore")
    }

    @Test("Parse schema with permuted index")
    func testParsePermutedIndex() throws {
        let yaml = """
        Event:
          #Directory: [events]

          id: string
          userId: string
          eventType: string
          timestamp: date

          #Index:
            - kind: permuted
              name: user_event_permuted
              fields: [userId, eventType]
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        let permutedIndex = catalog.indexes.first { $0.kindIdentifier == "permuted" }
        #expect(permutedIndex != nil)
        #expect(permutedIndex?.fieldNames == ["userId", "eventType"])
    }

    @Test("Parse schema with dynamic directory")
    func testParseDynamicDirectory() throws {
        let yaml = """
        Order:
          #Directory:
            - app
            - orders
            - field: tenantId

          id: string
          tenantId: string
          amount: double
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.directoryComponents.count == 3)
        if case .staticPath(let path) = catalog.directoryComponents[0] {
            #expect(path == "app")
        } else {
            Issue.record("Expected static path")
        }
        if case .dynamicField(let fieldName) = catalog.directoryComponents[2] {
            #expect(fieldName == "tenantId")
        } else {
            Issue.record("Expected dynamic field")
        }
    }

    @Test("Parse schema with optional and array types")
    func testParseComplexTypes() throws {
        let yaml = """
        User:
          #Directory: [app]

          id: string
          nickname: optional<string>
          tags: array<string>
          optionalTags: optional<array<string>>
        """

        let catalog = try SchemaFileParser.parseYAML(yaml)

        #expect(catalog.fields.count == 4)
        #expect(catalog.fields[1].isOptional == true)
        #expect(catalog.fields[2].isArray == true)
        #expect(catalog.fields[3].isOptional == true)
        #expect(catalog.fields[3].isArray == true)
    }

    @Test("Invalid type throws error")
    func testInvalidType() throws {
        let yaml = """
        User:
          #Directory: [app]

          id: invalidtype
        """

        #expect(throws: SchemaFileError.self) {
            try SchemaFileParser.parseYAML(yaml)
        }
    }
}
