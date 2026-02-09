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
/// - `shutdown()` stops the network explicitly. All `FDBDatabase` instances
///   must be released before calling this.
/// - If `shutdown()` is not called, the OS reclaims resources at process exit.
///   This is safe — FoundationDB is transactional.
///
/// ## Usage Example
/// ```swift
/// try await FDBClient.initialize()
/// let database = try FDBClient.openDatabase()
/// // ... use database ...
/// database = nil          // release database first
/// FDBClient.shutdown()    // then stop network
/// ```
final class FDBNetwork: Sendable {
    /// The shared singleton instance of the network manager.
    static let shared = FDBNetwork()

    /// Whether the network has been initialized.
    private let initialized = Mutex<Bool>(false)

    /// Number of active `FDBDatabase` instances.
    private let databaseCount = Mutex<Int>(0)

    /// Initializes the FoundationDB network with the specified API version.
    ///
    /// This method performs the complete network initialization sequence:
    /// selecting the API version, setting up the network, and starting the network thread.
    ///
    /// - Parameter version: The FoundationDB API version to use.
    /// - Throws: `FDBError` if any step of initialization fails.
    func initialize(version: Int) throws {
        try initialized.withLock { initialized in
            guard !initialized else {
                throw FDBError(.networkError)
            }
            try selectAPIVersion(Int32(version))
            try setupNetwork()
            startNetwork()
            initialized = true
        }
    }

    /// Registers a new `FDBDatabase` instance.
    ///
    /// Called by `FDBDatabase.init` to track active database connections.
    func trackDatabase() {
        databaseCount.withLock { $0 += 1 }
    }

    /// Unregisters an `FDBDatabase` instance.
    ///
    /// Called by `FDBDatabase.deinit` after `fdb_database_destroy`.
    func releaseDatabase() {
        databaseCount.withLock { $0 -= 1 }
    }

    /// Explicitly stops the FoundationDB network.
    ///
    /// All `FDBDatabase` instances must be released before calling this method.
    /// A `precondition` failure occurs if active databases remain.
    ///
    /// After calling this, `fdb_run_network()` returns on its thread.
    /// This method is optional — if not called, the OS reclaims all resources at process exit.
    func shutdown() {
        let activeCount = databaseCount.withLock { $0 }
        precondition(
            activeCount == 0,
            "All FDBDatabase instances must be released before shutdown. Active: \(activeCount)"
        )
        initialized.withLock { initialized in
            guard initialized else { return }
            let error = fdb_stop_network()
            if error != 0 {
                fatalError("Failed to stop network: \(FDBError(code: error).description)")
            }
            initialized = false
        }
    }

    /// Intentionally empty.
    ///
    /// At process exit, the destruction order of static variables is undefined.
    /// Calling `fdb_stop_network()` here while `FDBDatabase` instances are still
    /// alive causes a crash in the C++ runtime (DatabaseContext::~DatabaseContext).
    /// The OS reclaims all process resources safely.
    deinit {}

    /// Returns true if FDB network is initialized.
    public var isInitialized: Bool { initialized.withLock { $0 } }

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

    /// Starts the FoundationDB network thread.
    ///
    /// Creates a detached pthread that runs the FoundationDB network event loop.
    /// The thread runs until `fdb_stop_network()` is called or the process exits.
    private func startNetwork() {
        var thread = pthread_t(bitPattern: 0)
        pthread_create(&thread, nil, { _ in
            let error = fdb_run_network()
            if error != 0 {
                fatalError("Network thread error: \(FDBError(code: error).description)")
            }
            return nil
        }, nil)
        pthread_detach(thread!)
    }
}
