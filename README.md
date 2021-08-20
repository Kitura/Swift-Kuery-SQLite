<p align="center">
    <a href="https://www.kitura.dev">
        <img src="https://raw.githubusercontent.com/Kitura/Kitura/master/Sources/Kitura/resources/kitura-bird.svg?sanitize=true" height="100" alt="Kitura">
    </a>
</p>

<p align="center">
    <a href="https://kitura.github.io/Swift-Kuery-SQLite/index.html">
    <img src="https://img.shields.io/badge/apidoc-SwiftKuerySQLite-1FBCE4.svg?style=flat" alt="APIDoc">
    </a>
    <a href="https://travis-ci.org/Kitura/Swift-Kuery-SQLite">
    <img src="https://travis-ci.org/Kitura/Swift-Kuery-SQLite.svg?branch=master" alt="Build Status - Master">
    </a>
    <img src="https://img.shields.io/badge/os-macOS-green.svg?style=flat" alt="macOS">
    <img src="https://img.shields.io/badge/os-linux-green.svg?style=flat" alt="Linux">
    <img src="https://img.shields.io/badge/license-Apache2-blue.svg?style=flat" alt="Apache 2">
    <a href="https://slack.kitura.dev">
    <img src="http://swift-at-ibm-slack.mybluemix.net/badge.svg" alt="Slack Status">
    </a>
</p>

# Swift-Kuery-SQLite

[SQLite](https://sqlite.org/) plugin for the [Swift-Kuery](https://github.com/Kitura/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in an SQLite database.

## SQLite installation

To use Swift-Kuery-SQLite you must install SQLite.

### macOS

You can install SQLite with [Homebrew](https://brew.sh/):

```
$ brew install sqlite
```

Or, if you prefer MacPorts, you can use that too, though note that you need to symlink a file into the place that Homebrew installs it:

```
$ port install sqlite3
$ mkdir -p /usr/local/opt/sqlite/include
$ ln -s /opt/local/include/sqlite3.h /usr/local/opt/sqlite/include/
```

### Linux
```
$ sudo apt-get install sqlite3 libsqlite3-dev
```

## Usage

#### Add dependencies

Add the `Swift-Kuery-SQLite` package to the dependencies within your application’s `Package.swift` file. Substitute `"x.x.x"` with the latest `Swift-Kuery-SQLite` [release](https://github.com/Kitura/Swift-Kuery-SQLite/releases).

```swift
.package(url: "https://github.com/Kitura/Swift-Kuery-SQLite.git", from: "x.x.x")
```

Add `SwiftKuerySQLite` to your target's dependencies:

```swift
.target(name: "example", dependencies: ["SwiftKuerySQLite"]),
```

#### Import package

  ```swift
  import SwiftKuerySQLite
  ```

## Using Swift-Kuery-SQLite

First create an instance of `Swift-Kuery-SQLite` by calling:

```swift
let connection = SQLiteConnection(filename: "myDB.db")
```
You don't have to pass a filename, if you choose not to pass in a filename then your database will be in-memory.

To establish a connection call:

```swift
connection.connect() { result in
    guard result.success else {
        // Connection unsuccessful
        return
    }
    // Connection succesful
    // Use connection
}
```

If you want to have multiple connections to your database you can create a `ConnectionPool` as follows:

```swift
let pool = SQLiteConnection.createPool(filename: "myDB.db", poolOptions: ConnectionPoolOptions(initialCapacity: 10, maxCapacity: 30))

pool.getConnection() { connection, error in
    guard let connection = connection else {
        // Handle error
        return
    }
    // Use connection
}
```
Note, you don't have to pass a filename to the `createPool` method, if you choose not to pass in a filename then your pool will be shared in-memory.

You now have a connection that can be used to execute SQL queries created using Swift-Kuery. View the [Kuery](https://github.com/Kitura/Swift-Kuery) documentation for more information, or see the [Database Connectivity with Kuery](https://nocturnalsolutions.gitbooks.io/kitura-book/content/5-kuery.html) chapter of the *[Kitura Until Dawn](https://www.gitbook.com/book/nocturnalsolutions/kitura-book)* guidebook/tutorial.

## API Documentation
For more information visit our [API reference](https://kitura.github.io/Swift-Kuery-SQLite/index.html).

## Community

We love to talk server-side Swift, and Kitura. Join our [Slack](https://slack.kitura.dev) to meet the team!

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](https://github.com/Kitura/Swift-Kuery-SQLite/blob/master/LICENSE).
