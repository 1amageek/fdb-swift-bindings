/*
 * Package.swift
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

// swift-tools-version: 6.4
import PackageDescription

let package = Package(
    name: "FoundationDB",
    platforms: [
        .macOS(.v15),
    ],
    products: [
        .library(name: "FoundationDB", targets: ["FoundationDB"]),
    ],
    dependencies: [
        .package(
            url: "https://github.com/1amageek/database-types.git",
            from: "26.0728.0"
        ),
    ],
    targets: [
        .systemLibrary(
            name: "CFoundationDB",
            pkgConfig: "libfdb",
            providers: [
                .apt(["foundationdb-clients"]),
                .yum(["foundationdb-clients"]),
            ]
        ),
        .target(
            name: "FoundationDB",
            dependencies: [
                "CFoundationDB",
                .product(
                    name: "DatabaseTypes",
                    package: "database-types"
                ),
            ]
        ),
        .testTarget(
            name: "FoundationDBTests",
            dependencies: [
                "FoundationDB",
                "CFoundationDB",
                .product(
                    name: "DatabaseTypes",
                    package: "database-types"
                ),
            ],
            linkerSettings: [
                .unsafeFlags(["-L/usr/local/lib"]),
                .unsafeFlags(["-Xlinker", "-rpath", "-Xlinker", "/usr/local/lib"])
            ]
        ),
        .executableTarget(
            name: "StackTester",
            dependencies: [
                "FoundationDB",
                .product(
                    name: "DatabaseTypes",
                    package: "database-types"
                ),
            ],
            path: "Tests/StackTester/Sources/StackTester"
        ),
    ],
    swiftLanguageModes: [.v6]
)
