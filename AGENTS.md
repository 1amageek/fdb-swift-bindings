# AGENTS.md

## Responsibility

- This package owns the safe Swift API over the FoundationDB C client, including library lifecycle, database and transaction handles, future readiness, result decoding, retries, tuples, subspaces, and directories.
- It does not own higher-level database-framework semantics or StorageEngine policy.
- The public API must preserve FoundationDB's exact retry, transaction, cancellation, and result-presence semantics.

## Naming

- Name declarations for their FoundationDB-domain responsibility, observable behavior, event, ownership, or lifetime contract.
- Follow the Swift API Design Guidelines at every access level, including tests, C bridge support, and StackTester code.
- Do not encode Swift, C, CDecl, calling convention, module identity, binary layout, toolchain, or implementation strategy in ordinary declaration names.
- Keep externally fixed C symbol spellings at the import or attribute boundary only. Name Swift callbacks for the future event or state transition they deliver.
- A `FoundationDB` prefix is justified only when disambiguation is required by the public domain model, not because a C type had that prefix.
- Names such as `regular`, `legacy`, `impl`, `helper`, `manager`, or a bare `callback` are invalid.
- A synchronous borrowed input and the immutable retained `ByteString` from
  `DatabaseTypes` have distinct ownership contracts. Name the borrowed input
  for that contract and do not introduce a second retained byte type.

## Ownership, Retry, and Error Contracts

- Derive byte count and pointer within the same `withUnsafeBytes` borrow. Never perform a separate count read that creates a TOCTOU window.
- FoundationDB future results retain the future owner and expose bounded
  `ByteString` views without copying. A pointer must not escape its owner's
  lifetime.
- Snapshot mutable or arbitrary external ByteInput when immutable retained storage is required for hashing, selectors, or later use.
- Use FoundationDB's official error predicates for retry classification. Preserve the distinction between retryable-not-committed and maybe-committed outcomes.
- Complete every future continuation exactly once and propagate cancellation and typed FoundationDB errors without default success values.
- This is version 1. Remove obsolete aliases and compatibility overloads rather than preserving implementation history.
