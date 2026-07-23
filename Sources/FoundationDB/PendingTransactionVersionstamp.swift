extension FDB {
    /// A transaction versionstamp requested before the transaction commits.
    public protocol PendingTransactionVersionstamp: Sendable {
        /// Waits for the commit outcome and returns its assigned versionstamp.
        var value: TransactionVersionstamp { get async throws }
    }
}
