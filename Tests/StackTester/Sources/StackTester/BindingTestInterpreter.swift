/*
 * BindingTestInterpreter.swift
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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
@testable import FoundationDB

enum BindingTestInterpreterError: Error {
    case invalidInstruction
    case unsupportedInstruction(String)
    case missingRecordedTransactionVersion
    case invalidMutationName
    case unsupportedMutation(String)
}

// Value produced while executing one binding-test instruction.
struct StackEntry {
    let value: Any
    let instructionIndex: Int
}

class BindingTestInterpreter {
    private let programPrefix: [UInt8]
    private var stack: [StackEntry] = []
    private let database: FDBDatabase
    private let logsInstructions: Bool
    private var transactionsByName: [[UInt8]: any TransactionProtocol] = [:]
    private var selectedTransactionName: [UInt8] = Array("MAIN".utf8)
    private var recordedTransactionVersion: Int64?

    init(programPrefix: [UInt8], database: FDBDatabase, logsInstructions: Bool) {
        self.programPrefix = programPrefix
        self.database = database
        self.logsInstructions = logsInstructions
    }

    // Remove the most recently produced stack value.
    func popStackEntry() -> StackEntry {
        guard !stack.isEmpty else {
            fatalError("Stack is empty")
        }

        let entry = stack.removeLast()

        // Handle futures and convert types like in Go
        switch entry.value {
        case let data as [UInt8]:
            return StackEntry(value: data, instructionIndex: entry.instructionIndex)
        case let string as String:
            return StackEntry(value: Array(string.utf8), instructionIndex: entry.instructionIndex)
        case let int as Int64:
            return StackEntry(value: int, instructionIndex: entry.instructionIndex)
        default:
            return entry
        }
    }

    func pushStackValue(fromInstructionAt instructionIndex: Int, _ value: Any) {
        stack.append(StackEntry(value: value, instructionIndex: instructionIndex))
    }

    // Get current transaction (create if needed)
    func selectedTransaction() throws -> any TransactionProtocol {
        if let existingTransaction = transactionsByName[selectedTransactionName] {
            return existingTransaction
        }

        // Create new transaction if it doesn't exist
        let transaction = try database.createTransaction()
        transactionsByName[selectedTransactionName] = transaction
        return transaction
    }

    // Create a new transaction for the current transaction name
    func replaceSelectedTransaction() throws {
        let transaction = try database.createTransaction()
        transactionsByName[selectedTransactionName] = transaction
    }

    // Switch to a different transaction by name
    func selectTransaction(_ name: [UInt8]) throws {
        selectedTransactionName = name

        // Create transaction if it doesn't exist
        if transactionsByName[selectedTransactionName] == nil {
            try replaceSelectedTransaction()
        }
    }

    // Pack range results according to the binding-tester stack contract.
    func pushRangeRecords(
        _ instructionIndex: Int,
        _ records: FDB.KeyValueArray,
        prefixFilter: [UInt8]? = nil
    ) {
        var tupleElements: [any TupleElement] = []
        for (key, value) in records {
            if let prefix = prefixFilter {
                if key.starts(with: prefix) {
                    tupleElements.append(key.copyBytes())
                    tupleElements.append(value.copyBytes())
                }
            } else {
                tupleElements.append(key.copyBytes())
                tupleElements.append(value.copyBytes())
            }
        }
        let tuple = Tuple(tupleElements)
        pushStackValue(fromInstructionAt: instructionIndex, tuple.pack())
    }

    // Constrain a key result to the requested prefix range.
    func constrainKeyToPrefix(_ key: FDB.ByteString, prefix: [UInt8]) -> [UInt8] {
        if key.starts(with: prefix) {
            return key.copyBytes()
        } else if key.lexicographicallyPrecedes(prefix) {
            return prefix
        } else {
            return prefix + [0xFF]
        }
    }

    // Persist a batch of stack entries for binding-tester verification.
    func persistStackEntries(_ entries: [(stackIndex: Int, entry: StackEntry)], prefix: [UInt8]) async throws {
        try await database.withTransaction { transaction in
            for (stackIndex, entry) in entries {
                // Create key: prefix + tuple(stackIndex, entry.instructionIndex)
                let keyTuple = Tuple([Int64(stackIndex), Int64(entry.instructionIndex)])
                var key = prefix
                key.append(contentsOf: keyTuple.pack())

                // Pack value as a tuple (matching Python/Go behavior)
                let valueTuple: Tuple
                if let data = entry.value as? [UInt8] {
                    valueTuple = Tuple([data])
                } else if let str = entry.value as? String {
                    valueTuple = Tuple([str])
                } else if let int = entry.value as? Int64 {
                    valueTuple = Tuple([int])
                } else {
                    throw TupleError.unsupportedType
                }

                var packedValue = valueTuple.pack()

                // Limit value size to 40000 bytes
                let maximumValueByteCount = 40_000
                if packedValue.count > maximumValueByteCount {
                    packedValue = Array(packedValue.prefix(maximumValueByteCount))
                }

                try transaction.setValue(packedValue, for: key)
            }
            return ()
        }
    }

    // Process a single instruction - subset of Go's processInst
    func executeInstruction(_ instructionIndex: Int, _ instruction: [Any]) async throws {
        guard let operationName = instruction.first as? String else {
            throw BindingTestInterpreterError.invalidInstruction
        }

        if logsInstructions {
            print("\(instructionIndex). Instruction is \(operationName)")
            print("Stack: [\(stack.map { "\($0.value)" }.joined(separator: ", "))] (\(stack.count))")
        }

        switch operationName {
        case "PUSH":
            assert(instruction.count > 1)
            pushStackValue(fromInstructionAt: instructionIndex, instruction[1])

        case "POP":
            assert(!stack.isEmpty)
            _ = popStackEntry()

        case "DUP":
            assert(!stack.isEmpty)
            let entry = stack.last!
            pushStackValue(fromInstructionAt: entry.instructionIndex, entry.value)

        case "EMPTY_STACK":
            stack.removeAll()

        case "SWAP":
            assert(!stack.isEmpty)
            let swapIdx = popStackEntry().value as! Int64
            let lastIdx = stack.count - 1
            let targetIdx = lastIdx - Int(swapIdx)
            assert(targetIdx >= 0 && targetIdx < stack.count)
            stack.swapAt(lastIdx, targetIdx)

        case "SUB":
            assert(stack.count >= 2)
            let x = popStackEntry().value as! Int64
            let y = popStackEntry().value as! Int64
            pushStackValue(fromInstructionAt: instructionIndex, x - y)

        case "CONCAT":
            assert(stack.count >= 2)
            let str1 = popStackEntry().value
            let str2 = popStackEntry().value

            if let s1 = str1 as? String, let s2 = str2 as? String {
                pushStackValue(fromInstructionAt: instructionIndex, s1 + s2)
            } else if let d1 = str1 as? [UInt8], let d2 = str2 as? [UInt8] {
                pushStackValue(fromInstructionAt: instructionIndex, d1 + d2)
            } else {
                fatalError("Invalid CONCAT parameters")
            }

        case "NEW_TRANSACTION":
            try replaceSelectedTransaction()

        case "USE_TRANSACTION":
            let name = popStackEntry().value as! [UInt8]
            try selectTransaction(name)

        case "ON_ERROR":
            let errorCode = popStackEntry().value as! Int64
            let transaction = try selectedTransaction()

            // Create FDBError from the error code
            guard let typedErrorCode = Int32(exactly: errorCode) else {
                throw FDBError(.invalidAPICall)
            }
            let error = FDBError(code: typedErrorCode)

            // Call onError which will wait and handle the error appropriately
            do {
                try await transaction.onError(error)
                // If onError succeeds, the transaction has been reset and is ready to retry
                pushStackValue(fromInstructionAt: instructionIndex, Array("RESULT_NOT_PRESENT".utf8))
            } catch {
                // If onError fails, store the error (transaction should not be retried)
                throw error
            }

        case "GET_READ_VERSION":
            let transaction = try selectedTransaction()
            recordedTransactionVersion = try await transaction.getReadVersion()
            pushStackValue(fromInstructionAt: instructionIndex, Array("GOT_READ_VERSION".utf8))

        case "SET":
            assert(stack.count >= 2)
            let key = popStackEntry().value as! [UInt8]
            let value = popStackEntry().value as! [UInt8]

            try await database.withTransaction { transaction in
                try transaction.setValue(value, for: key)
                return ()
            }

        case "GET":
            assert(!stack.isEmpty)
            let key = popStackEntry().value as! [UInt8]

            let result = try await database.withTransaction { transaction in
                try await transaction.getValue(for: key, snapshot: false)
            }

            if let value = result {
                pushStackValue(fromInstructionAt: instructionIndex, value.copyBytes())
            } else {
                pushStackValue(fromInstructionAt: instructionIndex, Array("RESULT_NOT_PRESENT".utf8))
            }

        case "LOG_STACK":
            assert(!stack.isEmpty)
            let logPrefix = popStackEntry().value as! [UInt8]

            // Process stack in batches of 100 like Python/Go implementations
            var entries: [(stackIndex: Int, entry: StackEntry)] = []
            var stackIndex = stack.count - 1

            while !stack.isEmpty {
                let entry = popStackEntry()
                entries.append((stackIndex: stackIndex, entry: entry))
                stackIndex -= 1

                if entries.count == 100 {
                    try await persistStackEntries(entries, prefix: logPrefix)
                    entries.removeAll()
                }
            }

            // Log remaining entries
            if !entries.isEmpty {
                try await persistStackEntries(entries, prefix: logPrefix)
            }

        case "COMMIT":
            let transaction = try selectedTransaction()
            _ = try await transaction.commit()
            pushStackValue(fromInstructionAt: instructionIndex, Array("COMMIT_RESULT".utf8))

        case "RESET":
            if transactionsByName[selectedTransactionName] as? FDBTransaction != nil {
                try replaceSelectedTransaction()
            }

        case "CANCEL":
            if let transaction = transactionsByName[selectedTransactionName] {
                transaction.cancel()
            }

        case "GET_KEY":
            // Python order: key, or_equal, offset, prefix = inst.pop(4)
            let prefix = popStackEntry().value as! [UInt8]
            let offset = Int(popStackEntry().value as! Int64)
            let orEqual = (popStackEntry().value as! Int64) != 0
            let key = popStackEntry().value as! [UInt8]

            let selector = FDB.KeySelector(key: key, orEqual: orEqual, offset: offset)
            let transaction = try selectedTransaction()

            if let resultKey = try await transaction.getKey(selector: selector, snapshot: false) {
                let filteredKey = constrainKeyToPrefix(resultKey, prefix: prefix)
                pushStackValue(fromInstructionAt: instructionIndex, filteredKey)
            } else {
                pushStackValue(fromInstructionAt: instructionIndex, Array("RESULT_NOT_PRESENT".utf8))
            }

        case "GET_RANGE":
            // Python/Go order: begin, end, limit, reverse, mode (but Go pops in reverse)
            // Go pops: mode, reverse, limit, endKey, beginKey
            _ = popStackEntry().value as! Int64 // Streaming mode, ignore for now
            _ = (popStackEntry().value as! Int64) != 0
            let limit = Int(popStackEntry().value as! Int64)
            let endKey = popStackEntry().value as! [UInt8]
            let beginKey = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()

            let result = try await transaction.readRangeBatch(
                from: beginKey,
                to: endKey,
                limit: limit,
                snapshot: false
            )

            pushRangeRecords(instructionIndex, result.records)

        case "GET_RANGE_STARTS_WITH":
            // Python order: prefix, limit, reverse, mode (pops 4 parameters)
            // Go order: same but pops in reverse
            _ = popStackEntry().value as! Int64 // Streaming mode, ignore for now
            _ = (popStackEntry().value as! Int64) != 0
            let limit = Int(popStackEntry().value as! Int64)
            let prefix = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()

            var endKey = prefix
            endKey.append(0xFF)

            let result = try await transaction.readRangeBatch(
                from: prefix,
                to: endKey,
                limit: limit,
                snapshot: false
            )

            pushRangeRecords(instructionIndex, result.records)

        case "GET_RANGE_SELECTOR":
            // Python pops 10 parameters: begin_key, begin_or_equal, begin_offset, end_key, end_or_equal, end_offset, limit, reverse, mode, prefix
            // Go pops in reverse order
            let prefix = popStackEntry().value as! [UInt8]
            _ = popStackEntry().value as! Int64 // Streaming mode, ignore for now
            _ = (popStackEntry().value as! Int64) != 0
            let limit = Int(popStackEntry().value as! Int64)
            let endOffset = Int(popStackEntry().value as! Int64)
            let endOrEqual = (popStackEntry().value as! Int64) != 0
            let endKey = popStackEntry().value as! [UInt8]
            let beginOffset = Int(popStackEntry().value as! Int64)
            let beginOrEqual = (popStackEntry().value as! Int64) != 0
            let beginKey = popStackEntry().value as! [UInt8]

            let beginSelector = FDB.KeySelector(key: beginKey, orEqual: beginOrEqual, offset: beginOffset)
            let endSelector = FDB.KeySelector(key: endKey, orEqual: endOrEqual, offset: endOffset)
            let transaction = try selectedTransaction()

            let result = try await transaction.readRangeBatch(
                from: beginSelector,
                to: endSelector,
                limit: limit,
                targetBytes: 0,
                streamingMode: .iterator,
                iteration: 1,
                reverse: false,
                snapshot: false
            )

            pushRangeRecords(instructionIndex, result.records, prefixFilter: prefix)

        case "GET_ESTIMATED_RANGE_SIZE":
            let endKey = popStackEntry().value as! [UInt8]
            let beginKey = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()

            _ = try await transaction.getEstimatedRangeSizeBytes(beginKey: beginKey, endKey: endKey)
            pushStackValue(fromInstructionAt: instructionIndex, Array("GOT_ESTIMATED_RANGE_SIZE".utf8))

        case "GET_RANGE_SPLIT_POINTS":
            let chunkSize = Int(popStackEntry().value as! Int64)
            let endKey = popStackEntry().value as! [UInt8]
            let beginKey = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()

            _ = try await transaction.getRangeSplitPoints(beginKey: beginKey, endKey: endKey, chunkSize: chunkSize)
            pushStackValue(fromInstructionAt: instructionIndex, Array("GOT_RANGE_SPLIT_POINTS".utf8))

        case "CLEAR":
            let key = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()
            try transaction.clear(key: key)

        case "CLEAR_RANGE":
            let beginKey = popStackEntry().value as! [UInt8]
            let endKey = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()
            try transaction.clearRange(beginKey: beginKey, endKey: endKey)

        case "CLEAR_RANGE_STARTS_WITH":
            let prefix = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()
            var endKey = prefix
            endKey.append(0xFF)
            try transaction.clearRange(beginKey: prefix, endKey: endKey)

        case "ATOMIC_OP":
            // Binding contract order: mutation name, key, parameter.
            let param = popStackEntry().value as! [UInt8] // value/param
            let key = popStackEntry().value as! [UInt8] // key
            let mutationNameBytes = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()

            guard let mutationName = String(bytes: mutationNameBytes, encoding: .utf8) else {
                throw BindingTestInterpreterError.invalidMutationName
            }
            let mutationType: FDB.MutationType
            switch mutationName {
            case "ADD":
                mutationType = .add
            case "BIT_AND":
                mutationType = .bitAnd
            case "BIT_OR":
                mutationType = .bitOr
            case "BIT_XOR":
                mutationType = .bitXor
            case "APPEND_IF_FITS":
                mutationType = .appendIfFits
            case "MAX":
                mutationType = .max
            case "MIN":
                mutationType = .min
            case "SET_VERSIONSTAMPED_KEY":
                mutationType = .setVersionstampedKey
            case "SET_VERSIONSTAMPED_VALUE":
                mutationType = .setVersionstampedValue
            case "BYTE_MIN":
                mutationType = .byteMin
            case "BYTE_MAX":
                mutationType = .byteMax
            case "COMPARE_AND_CLEAR":
                mutationType = .compareAndClear
            default:
                throw BindingTestInterpreterError.unsupportedMutation(mutationName)
            }

            try transaction.atomicOp(
                key: key,
                param: param,
                mutationType: mutationType
            )

        case "SET_READ_VERSION":
            guard let version = recordedTransactionVersion else {
                throw BindingTestInterpreterError.missingRecordedTransactionVersion
            }
            let transaction = try selectedTransaction()
            transaction.setReadVersion(version)

        case "GET_COMMITTED_VERSION":
            let transaction = try selectedTransaction()
            recordedTransactionVersion = try transaction.getCommittedVersion()
            pushStackValue(fromInstructionAt: instructionIndex, Array("GOT_COMMITTED_VERSION".utf8))

        case "GET_APPROXIMATE_SIZE":
            let transaction = try selectedTransaction()
            _ = try await transaction.approximateSize()
            pushStackValue(fromInstructionAt: instructionIndex, Array("GOT_APPROXIMATE_SIZE".utf8))

        case "GET_VERSIONSTAMP":
            let transaction = try selectedTransaction()
            if let versionstamp = try await transaction.getVersionstamp() {
                pushStackValue(fromInstructionAt: instructionIndex, versionstamp)
            } else {
                pushStackValue(fromInstructionAt: instructionIndex, Array("RESULT_NOT_PRESENT".utf8))
            }

        case "READ_CONFLICT_RANGE":
            let endKey = popStackEntry().value as! [UInt8]
            let beginKey = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()
            try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: .read)

        case "WRITE_CONFLICT_RANGE":
            let endKey = popStackEntry().value as! [UInt8]
            let beginKey = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()
            try transaction.addConflictRange(beginKey: beginKey, endKey: endKey, type: .write)

        case "READ_CONFLICT_KEY":
            let key = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()
            // For a single key, create a range [key, key+\x00)
            var endKey = key
            endKey.append(0x00)
            try transaction.addConflictRange(beginKey: key, endKey: endKey, type: .read)

        case "WRITE_CONFLICT_KEY":
            let key = popStackEntry().value as! [UInt8]
            let transaction = try selectedTransaction()
            // For a single key, create a range [key, key+\x00)
            var endKey = key
            endKey.append(0x00)
            try transaction.addConflictRange(beginKey: key, endKey: endKey, type: .write)

        case "DISABLE_WRITE_CONFLICT":
            // Not directly available in Swift bindings, could use transaction option
            let transaction = try selectedTransaction()
            try transaction.setOption(forOption: .nextWriteNoWriteConflictRange)

        case "TUPLE_PACK":
            let elementCount = popStackEntry().value as! Int64
            var elements: [any TupleElement] = []

            for _ in 0 ..< elementCount {
                let item = popStackEntry().value
                guard let element = item as? any TupleElement else {
                    throw TupleError.unsupportedType
                }
                elements.append(element)
            }

            let tuple = Tuple(elements.reversed()) // Reverse because we popped in reverse order
            pushStackValue(fromInstructionAt: instructionIndex, tuple.pack())

        case "TUPLE_PACK_WITH_VERSIONSTAMP":
            // Python order: prefix, count, items
            let prefix = popStackEntry().value as! [UInt8]
            let elementCount = popStackEntry().value as! Int64
            var elements: [any TupleElement] = []

            for _ in 0 ..< elementCount {
                let item = popStackEntry().value
                guard let element = item as? any TupleElement else {
                    throw TupleError.unsupportedType
                }
                elements.append(element)
            }

            let tuple = Tuple(elements.reversed())
            do {
                let result = try tuple.packWithVersionstamp(prefix: prefix)
                pushStackValue(fromInstructionAt: instructionIndex, Array("OK".utf8))
                pushStackValue(fromInstructionAt: instructionIndex, result)
            } catch TupleError.missingIncompleteVersionstamp {
                pushStackValue(fromInstructionAt: instructionIndex, Array("ERROR: NONE".utf8))
            } catch TupleError.multipleIncompleteVersionstamps {
                pushStackValue(fromInstructionAt: instructionIndex, Array("ERROR: MULTIPLE".utf8))
            }

        case "TUPLE_UNPACK":
            let encodedTuple = popStackEntry().value as! [UInt8]
            let elements = try Tuple.unpack(from: encodedTuple)
            for element in elements.reversed() { // Reverse to match stack order
                pushStackValue(fromInstructionAt: instructionIndex, element.encodeTuple())
            }

        case "TUPLE_SORT":
            let tupleCount = popStackEntry().value as! Int64
            var tuples: [[UInt8]] = []

            for _ in 0 ..< tupleCount {
                tuples.append(popStackEntry().value as! [UInt8])
            }

            tuples.sort { $0.lexicographicallyPrecedes($1) }

            for tuple in tuples {
                pushStackValue(fromInstructionAt: instructionIndex, tuple)
            }

        case "TUPLE_RANGE":
            let elementCount = popStackEntry().value as! Int64
            var elements: [any TupleElement] = []

            for _ in 0 ..< elementCount {
                let item = popStackEntry().value
                guard let element = item as? any TupleElement else {
                    throw TupleError.unsupportedType
                }
                elements.append(element)
            }

            let tuple = Tuple(elements.reversed())
            let prefix = tuple.pack()

            // Create range: prefix to prefix + [0xFF]
            var endKey = prefix
            endKey.append(0xFF)

            pushStackValue(fromInstructionAt: instructionIndex, prefix)
            pushStackValue(fromInstructionAt: instructionIndex, endKey)

        case "ENCODE_FLOAT":
            let floatValue = Float(popStackEntry().value as! Int64) // Convert from int representation
            let data = withUnsafeBytes(of: floatValue.bitPattern) { Array($0) }
            pushStackValue(fromInstructionAt: instructionIndex, data)

        case "ENCODE_DOUBLE":
            let doubleValue = Double(popStackEntry().value as! Int64) // Convert from int representation
            let data = withUnsafeBytes(of: doubleValue.bitPattern) { Array($0) }
            pushStackValue(fromInstructionAt: instructionIndex, data)

        case "DECODE_FLOAT":
            let data = popStackEntry().value as! [UInt8]
            guard data.count == MemoryLayout<Float>.size else {
                throw TupleError.invalidDecoding(
                    "DECODE_FLOAT requires exactly \(MemoryLayout<Float>.size) bytes"
                )
            }
            let floatValue = data.withUnsafeBytes { $0.loadUnaligned(as: Float.self) }
            pushStackValue(fromInstructionAt: instructionIndex, Int64(floatValue.bitPattern))

        case "DECODE_DOUBLE":
            let data = popStackEntry().value as! [UInt8]
            guard data.count == MemoryLayout<Double>.size else {
                throw TupleError.invalidDecoding(
                    "DECODE_DOUBLE requires exactly \(MemoryLayout<Double>.size) bytes"
                )
            }
            let doubleValue = data.withUnsafeBytes { $0.loadUnaligned(as: Double.self) }
            pushStackValue(fromInstructionAt: instructionIndex, Int64(bitPattern: doubleValue.bitPattern))

        case "WAIT_FUTURE":
            // In async context, futures are automatically awaited, just pass through the item
            let oldIdx = stack.count > 0 ? stack.last!.instructionIndex : instructionIndex
            let item = popStackEntry().value
            pushStackValue(fromInstructionAt: oldIdx, item)

        case "START_THREAD":
            throw BindingTestInterpreterError.unsupportedInstruction(operationName)

        case "WAIT_EMPTY":
            // Wait until stack is empty - already satisfied since we process sequentially
            break

        case "UNIT_TESTS":
            throw BindingTestInterpreterError.unsupportedInstruction(operationName)

        default:
            throw BindingTestInterpreterError.unsupportedInstruction(operationName)
        }

        if logsInstructions {
            print("        -> [\(stack.map { "\($0.value)" }.joined(separator: ", "))] (\(stack.count))")
            print()
        }
    }

    // Main run function - equivalent to Go's Run()
    func executeProgram() async throws {
        // Read instructions from the database using the program prefix.
        let instructions = try await database.withTransaction { transaction -> [(key: [UInt8], value: [UInt8])] in
            let prefixTuple = Tuple([programPrefix])
            let beginKey = prefixTuple.pack()
            let endKey = beginKey + [0xFF] // Simple range end

            let result = try await transaction.readRangeBatch(
                from: beginKey,
                to: endKey,
                limit: 0,
                snapshot: false
            )

            return result.records.map {
                (key: $0.0.copyBytes(), value: $0.1.copyBytes())
            }
        }

        if logsInstructions {
            print("Found \(instructions.count) instructions")
        }

        // Process each instruction
        for (i, (_, value)) in instructions.enumerated() {
            // Unpack the instruction tuple from the value
            let elements = try Tuple.unpack(from: value)

            // Convert tuple elements to array for processing
            var instruction: [Any] = []
            for element in elements {
                if let stringElement = element as? String {
                    instruction.append(stringElement)
                } else if let bytesElement = element as? [UInt8] {
                    instruction.append(bytesElement)
                } else if let intElement = element as? Int64 {
                    instruction.append(intElement)
                } else {
                    instruction.append(element)
                }
            }

            if logsInstructions {
                print("Instruction \(i): \(instruction)")
            }

            try await executeInstruction(i, instruction)
        }

        print("StackTester completed successfully with \(instructions.count) instructions")
    }
}
