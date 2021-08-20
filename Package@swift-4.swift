// swift-tools-version:4.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

/**
 * Copyright IBM Corporation and the Kitura project authors 2016-2020
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

import PackageDescription

let package = Package(
    name: "SwiftKuerySQLite",
    products: [
        .library(
            name: "SwiftKuerySQLite",
            targets: ["SwiftKuerySQLite"]
        )
    ],
    dependencies: [
        //.package(url: "https://github.com/Kitura/Swift-Kuery.git", from: "3.1.0"),
        .package(url: "https://github.com/Kitura/Swift-Kuery.git", .branch("master")),
    ],
    targets: [
        .target(
            name: "SwiftKuerySQLite",
            dependencies: ["SwiftKuery", "CSQLite"]
        ),
        .target(
            name: "CSQLite",
            dependencies: []
        ),
        .testTarget(
            name: "SwiftKuerySQLiteTests",
            dependencies: ["SwiftKuerySQLite"]
        )
    ]
)
