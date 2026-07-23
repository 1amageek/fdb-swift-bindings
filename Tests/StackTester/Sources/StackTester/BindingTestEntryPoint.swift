/*
 * BindingTestEntryPoint.swift
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
import FoundationDB

@main
struct BindingTestEntryPoint {
    static func main() async {
        guard CommandLine.arguments.count >= 3 else {
            print("Usage: stacktester <prefix> <api_version> [cluster_file]")
            exit(1)
        }

        let programPrefix = Array(CommandLine.arguments[1].utf8)
        let apiVersionString = CommandLine.arguments[2]
        let clusterFilePath = CommandLine.arguments.count > 3
            ? CommandLine.arguments[3]
            : nil

        guard let apiVersion = Int(apiVersionString) else {
            print("Invalid API version: \(apiVersionString)")
            exit(1)
        }

        do {
            try await FDBClient.initialize(version: apiVersion)
            let database = try FDBClient.openDatabase(clusterFilePath: clusterFilePath)
            let interpreter = BindingTestInterpreter(
                programPrefix: programPrefix,
                database: database,
                logsInstructions: false
            )
            try await interpreter.executeProgram()
            print("Binding test completed successfully")
        } catch {
            print("Binding test failed: \(error)")
            exit(1)
        }
    }
}
