/// Owns a transaction versionstamp future from request through commit.
final class TransactionVersionstampRequest:
        FDB.PendingTransactionVersionstamp,
        Sendable {
    private let future: Future<TransactionVersionstampResultDecoder>

    init(future: Future<TransactionVersionstampResultDecoder>) {
        self.future = future
    }

    var value: FDB.TransactionVersionstamp {
        get async throws {
            try await future.value
        }
    }
}
