/*
 * Network.swift
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
import CFoundationDB
import Synchronization

#if canImport(Darwin)
    import Darwin
#elseif canImport(Glibc)
    import Glibc
#endif

/// Singleton network manager for FoundationDB operations.
///
/// `FDBNetwork` manages the FoundationDB network layer, including initialization,
/// network thread management, and network option configuration. It follows the
/// singleton pattern to ensure only one network instance exists per process.
///
/// ## Lifecycle
///
/// - `initialize(version:)` starts the network thread (once per process).
/// - `shutdown()` stops the network and waits for the network thread to exit
///   via `pthread_join`. All `FDBDatabase` instances should be released before
///   calling this.
/// - The FDB C API does not support re-initialization after shutdown.
///
/// ## Usage Example
/// ```swift
/// try await FDBClient.initialize()
/// let database = try FDBClient.openDatabase()
/// // ... use database ...
/// database = nil          // release database first
/// FDBClient.shutdown()    // stops network, joins thread, process can exit
/// ```
final class FDBNetwork: Sendable {

    /// Internal lifecycle state.
    ///
    /// `@unchecked Sendable` because `pthread_t` is not `Sendable`,
    /// but all access is guarded by `Mutex`.
    private struct State: @unchecked Sendable {
        var phase: Phase = .uninitialized
        var databaseCount: Int = 0
        var networkThread: pthread_t? = nil

        enum Phase: Sendable {
            case uninitialized
            case running
            case stopped
        }
    }

    /// The shared singleton instance of the network manager.
    static let shared = FDBNetwork()

    /// All mutable state consolidated into a single Mutex.
    private let state = Mutex<State>(State())

    /// Initializes the FoundationDB network with the specified API version.
    ///
    /// This method performs the complete network initialization sequence:
    /// selecting the API version, setting up the network, and starting the network thread.
    /// Can only be called once per process.
    ///
    /// - Parameter version: The FoundationDB API version to use.
    /// - Throws: `FDBError` if any step of initialization fails.
    func initialize(version: Int) throws {
        try state.withLock { state in
            switch state.phase {
            case .uninitialized:
                try selectAPIVersion(Int32(version))
                try setupNetwork()
                startNetwork(state: &state)
                state.phase = .running
            case .running:
                throw FDBError(.networkError)
            case .stopped:
                fatalError(
                    "FDB network cannot be restarted after shutdown. "
                    + "This is a FoundationDB C API limitation."
                )
            }
        }
    }

    /// Registers a new `FDBDatabase` instance.
    ///
    /// Called by `FDBDatabase.init` to track active database connections.
    func trackDatabase() {
        state.withLock { state in
            assert(
                state.phase == .running,
                "trackDatabase() called when network is not running (phase: \(state.phase))"
            )
            state.databaseCount += 1
        }
    }

    /// Unregisters an `FDBDatabase` instance.
    ///
    /// Called by `FDBDatabase.deinit` after `fdb_database_destroy`.
    func releaseDatabase() {
        state.withLock { state in
            state.databaseCount -= 1
        }
    }

    /// Explicitly stops the FoundationDB network and waits for the
    /// network thread to exit.
    ///
    /// All `FDBDatabase` instances should be released before calling this.
    /// In debug builds, an assertion failure occurs if active databases remain.
    ///
    /// This method is idempotent — calling it multiple times is safe.
    ///
    /// After this method returns, the network thread has fully exited and
    /// the process can terminate naturally without `Foundation.exit()`.
    func shutdown() {
        // Phase 1: Under lock — signal stop, extract thread handle
        let thread: pthread_t? = state.withLock { state in
            guard state.phase == .running else { return nil }

            if state.databaseCount > 0 {
                assertionFailure(
                    "FDBNetwork.shutdown() called with \(state.databaseCount) active "
                    + "FDBDatabase instance(s). Release all databases before shutdown."
                )
            }

            let error = fdb_stop_network()
            if error != 0 {
                fatalError(
                    "Failed to stop FDB network: \(FDBError(code: error).description)"
                )
            }

            state.phase = .stopped
            let handle = state.networkThread
            state.networkThread = nil
            return handle
        }

        // Phase 2: Outside lock — join the network thread.
        // Must be outside the lock to avoid deadlock: the network thread
        // may call releaseDatabase() during its wind-down, which acquires
        // the same Mutex.
        if let thread {
            pthread_join(thread, nil)
        }
    }

    /// Intentionally empty.
    ///
    /// At process exit, the destruction order of static variables is undefined.
    /// Calling `fdb_stop_network()` here while other resources are still alive
    /// causes crashes. The OS reclaims all process resources safely.
    deinit {}

    /// Returns true if FDB network is initialized and running.
    public var isInitialized: Bool {
        state.withLock { $0.phase == .running }
    }

    // MARK: - Network Options

    /// Sets a network option with an optional byte array value.
    ///
    /// - Parameters:
    ///   - value: Optional byte array value for the option.
    ///   - option: The network option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    func setNetworkOption(to value: [UInt8]? = nil, forOption option: FDB.NetworkOption) throws {
        let error: Int32
        if let value = value {
            error = value.withUnsafeBytes { bytes in
                fdb_network_set_option(
                    FDBNetworkOption(option.rawValue),
                    bytes.bindMemory(to: UInt8.self).baseAddress,
                    Int32(value.count)
                )
            }
        } else {
            error = fdb_network_set_option(FDBNetworkOption(option.rawValue), nil, 0)
        }

        if error != 0 {
            throw FDBError(code: error)
        }
    }

    /// Sets a network option with a string value.
    ///
    /// - Parameters:
    ///   - value: String value for the option (automatically converted to UTF-8 bytes).
    ///   - option: The network option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    func setNetworkOption(to value: String, forOption option: FDB.NetworkOption) throws {
        try setNetworkOption(to: [UInt8](value.utf8), forOption: option)
    }

    /// Sets a network option with an integer value.
    ///
    /// - Parameters:
    ///   - value: Integer value for the option (automatically converted to 64-bit bytes).
    ///   - option: The network option to set.
    /// - Throws: `FDBError` if the option cannot be set.
    func setNetworkOption(to value: Int, forOption option: FDB.NetworkOption) throws {
        let valueBytes = withUnsafeBytes(of: Int64(value)) { [UInt8]($0) }
        try setNetworkOption(to: valueBytes, forOption: option)
    }

    // MARK: - Private

    /// Selects the FoundationDB API version.
    ///
    /// - Parameter version: The API version to select.
    /// - Throws: `FDBError` if the API version cannot be selected.
    private func selectAPIVersion(_ version: Int32) throws {
        let error = fdb_select_api_version_impl(version, FDB_API_VERSION)
        if error != 0 {
            throw FDBError(code: error)
        }
    }

    /// Sets up the FoundationDB network layer.
    ///
    /// This method must be called before starting the network thread.
    ///
    /// - Throws: `FDBError` if network setup fails or if already set up.
    private func setupNetwork() throws {
        let error = fdb_setup_network()
        if error != 0 {
            throw FDBError(code: error)
        }
    }

    /// Starts the FoundationDB network thread (joinable, NOT detached).
    ///
    /// The thread handle is stored in `state.networkThread` so it can be
    /// joined during `shutdown()`.
    ///
    /// - Parameter state: Mutable reference to the current state (must be called under lock).
    private func startNetwork(state: inout State) {
        var thread: pthread_t?
        let result = pthread_create(&thread, nil, { _ in
            let error = fdb_run_network()
            if error != 0 {
                fatalError("Network thread error: \(FDBError(code: error).description)")
            }
            return nil
        }, nil)
        precondition(result == 0, "Failed to create FDB network thread (errno: \(result))")
        state.networkThread = thread
    }
}
