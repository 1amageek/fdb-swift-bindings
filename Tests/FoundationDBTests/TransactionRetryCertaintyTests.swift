import Synchronization
import Testing

@testable import FoundationDB

@Test("FoundationDB size-limit errors retain typed non-retryable semantics")
func sizeLimitErrorsAreTypedAndNonRetryable() {
    let cases: [(FDBErrorCode, String)] = [
        (.transactionTooLarge, "Transaction exceeds byte limit"),
        (.keyTooLarge, "Key length exceeds limit"),
        (.valueTooLarge, "Value length exceeds limit"),
    ]

    for (code, expectedDescription) in cases {
        let error = FDBError(code: code.rawValue)
        #expect(error.knownCode == code)
        #expect(error.retryDisposition == .never)
        #expect(error.description == expectedDescription)
    }
}

@Test("FoundationDB runtime predicates preserve commit certainty")
func runtimePredicatesPreserveCommitCertainty() {
    let definitelyNotCommittedCodes: [Int32] = [
        FDBErrorCode.notCommitted.rawValue,
        FDBErrorCode.transactionTooOld.rawValue,
        FDBErrorCode.futureVersion.rawValue,
        FDBErrorCode.processBehind.rawValue,
        FDBErrorCode.tagThrottled.rawValue,
        1038,
        1042,
        1051,
        1078,
        1223,
    ]
    for code in definitelyNotCommittedCodes {
        #expect(FDBError(code: code).retryDisposition == .retryableNotCommitted)
    }

    let uncertainCommitCodes: [Int32] = [
        FDBErrorCode.commitUnknownResult.rawValue,
        1039,
    ]
    for code in uncertainCommitCodes {
        #expect(FDBError(code: code).retryDisposition == .maybeCommitted)
    }

    #expect(FDBError(.transactionCancelled).retryDisposition == .never)
    #expect(FDBError(.internalError).retryDisposition == .never)
    #expect(FDBError(.operationTimedOut).retryDisposition == .never)
    #expect(FDBError(.idempotencyStatusUnknown).retryDisposition == .never)
    #expect(FDBError(.transactionTimedOut).retryDisposition == .never)
    #expect(FDBError(code: 314_159).retryDisposition == .never)
}

@Test("Commit-unknown result never replays the transaction body")
func commitUnknownDoesNotReplayTransactionBody() async throws {
    let transaction = RetryCertaintyTransaction(
        commitErrors: [FDBErrorCode.commitUnknownResult.rawValue]
    )
    let database = RetryCertaintyDatabase(transaction: transaction)

    do {
        _ = try await database.withTransaction { _ in
            transaction.recordBodyExecution()
            return 1
        }
        Issue.record("Expected commit-unknown failure")
    } catch let error as FDBError {
        #expect(error.code == FDBErrorCode.commitUnknownResult.rawValue)
    } catch {
        Issue.record("Expected FDBError, got \(error)")
    }

    #expect(transaction.bodyExecutionCount == 1)
    #expect(transaction.commitCount == 1)
    #expect(transaction.onErrorCount == 0)
    #expect(transaction.cancelCount == 1)
}

@Test("Known not-committed result resets and replays once")
func notCommittedRetriesTransactionBody() async throws {
    let transaction = RetryCertaintyTransaction(
        commitErrors: [FDBErrorCode.notCommitted.rawValue]
    )
    let database = RetryCertaintyDatabase(transaction: transaction)

    let result = try await database.withTransaction { _ in
        transaction.recordBodyExecution()
        return 42
    }

    #expect(result == 42)
    #expect(transaction.bodyExecutionCount == 2)
    #expect(transaction.commitCount == 2)
    #expect(transaction.onErrorCount == 1)
    #expect(transaction.cancelCount == 0)
}

@Test("Definitely uncommitted read failure replays the transaction body")
func definitelyUncommittedReadFailureReplaysTransactionBody() async throws {
    let transaction = RetryCertaintyTransaction(commitErrors: [])
    let database = RetryCertaintyDatabase(transaction: transaction)

    let result = try await database.withTransaction { _ in
        transaction.recordBodyExecution()
        if transaction.bodyExecutionCount == 1 {
            throw FDBError(code: 1038)
        }
        return 7
    }

    #expect(result == 7)
    #expect(transaction.bodyExecutionCount == 2)
    #expect(transaction.commitCount == 1)
    #expect(transaction.onErrorCount == 1)
    #expect(transaction.cancelCount == 0)
}

private struct RetryCertaintyDatabase: DatabaseProtocol {
    let transaction: RetryCertaintyTransaction

    func createTransaction() throws -> RetryCertaintyTransaction {
        transaction
    }
}

private final class RetryCertaintyTransaction: TransactionProtocol, Sendable {
    private struct State: Sendable {
        var remainingCommitErrors: [Int32]
        var bodyExecutionCount = 0
        var commitCount = 0
        var onErrorCount = 0
        var cancelCount = 0
    }

    private let state: Mutex<State>

    init(commitErrors: [Int32]) {
        self.state = Mutex(State(remainingCommitErrors: commitErrors))
    }

    var bodyExecutionCount: Int {
        state.withLock { $0.bodyExecutionCount }
    }

    var commitCount: Int {
        state.withLock { $0.commitCount }
    }

    var onErrorCount: Int {
        state.withLock { $0.onErrorCount }
    }

    var cancelCount: Int {
        state.withLock { $0.cancelCount }
    }

    func recordBodyExecution() {
        state.withLock { $0.bodyExecutionCount += 1 }
    }

    func getValue<Key: FDB.ByteInput>(
        for key: Key,
        snapshot: Bool
    ) async throws -> FDB.ByteString? {
        nil
    }

    func setValue<Value: FDB.ByteInput, Key: FDB.ByteInput>(
        _ value: Value,
        for key: Key
    ) throws {}

    func clear<Key: FDB.ByteInput>(key: Key) throws {}

    func clearRange<Begin: FDB.ByteInput, End: FDB.ByteInput>(
        beginKey: Begin,
        endKey: End
    ) throws {}

    func getKey(
        selector: FDB.KeySelector,
        snapshot: Bool
    ) async throws -> FDB.ByteString? {
        nil
    }

    func readRangeBatch(
        from begin: FDB.KeySelector,
        to end: FDB.KeySelector,
        limit: Int,
        targetBytes: Int,
        streamingMode: FDB.StreamingMode,
        iteration: Int,
        reverse: Bool,
        snapshot: Bool
    ) async throws -> RangeBatch {
        fatalError("Native range reads are outside this retry test")
    }

    func commit() async throws -> Bool {
        let errorCode = state.withLock { state -> Int32? in
            state.commitCount += 1
            guard !state.remainingCommitErrors.isEmpty else {
                return nil
            }
            return state.remainingCommitErrors.removeFirst()
        }
        if let errorCode {
            throw FDBError(code: errorCode)
        }
        return true
    }

    func cancel() {
        state.withLock { $0.cancelCount += 1 }
    }

    func getVersionstamp() async throws -> FDB.ByteString? {
        nil
    }

    func setReadVersion(_ version: FDB.Version) {}

    func getReadVersion() async throws -> FDB.Version {
        0
    }

    func onError(_ error: FDBError) async throws {
        state.withLock { $0.onErrorCount += 1 }
    }

    func getEstimatedRangeSizeBytes<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End
    ) async throws -> Int {
        0
    }

    func getRangeSplitPoints<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        chunkSize: Int
    ) async throws -> [FDB.ByteString] {
        []
    }

    func getCommittedVersion() throws -> FDB.Version {
        0
    }

    func approximateSize() async throws -> Int64 {
        0
    }

    func atomicOp<Key: FDB.ByteInput, Parameter: FDB.ByteInput>(
        key: Key,
        param: Parameter,
        mutationType: FDB.MutationType
    ) throws {}

    func addConflictRange<
        Begin: FDB.ByteInput,
        End: FDB.ByteInput
    >(
        beginKey: Begin,
        endKey: End,
        type: FDB.ConflictRangeType
    ) throws {}

    func setOption<Value: FDB.ByteInput>(
        to value: Value,
        forOption option: FDB.TransactionOption
    ) throws {}

    func setOption(forOption option: FDB.TransactionOption) throws {}

    func setOption(
        to value: String,
        forOption option: FDB.TransactionOption
    ) throws {}

    func setOption(
        to value: Int,
        forOption option: FDB.TransactionOption
    ) throws {}
}
