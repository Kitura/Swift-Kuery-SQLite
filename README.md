# Swift-Kuery-SQLite

SQLite plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework.

This is a fork of the [original project](https://github.com/IBM-Swift-Sunset/Swift-Kuery-SQLite), which was ”sunsetted“ rather than being ported to Swift 4. But I have updated it for Swift 4, so… I guess I'm maintaining it now?

<!-- [![Build Status - Master](https://travis-ci.org/IBM-Swift/Kitura.svg?branch=master)](https://travis-ci.org/IBM-Swift/Swift-Kuery-SQLite) /-->
![macOS](https://img.shields.io/badge/os-macOS-green.svg?style=flat)
![Linux](https://img.shields.io/badge/os-linux-green.svg?style=flat)
![Apache 2](https://img.shields.io/badge/license-Apache2-blue.svg?style=flat)

## Summary
[SQLite](https://sqlite.org/) plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in an SQLite database.

## SQLite installation

To use Swift-Kuery-SQLite you must install SQLite.

### macOS

You can install SQLite with Homebrew:

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

## Using Swift-Kuery-SQLite

First create an instance of `Swift-Kuery-SQLite` by calling:

```swift
let db = SQLiteConnection(filename: "myDB.db")
```

To establish a connection call:

```swift
db.connect(onCompletion: (QueryError?) -> ())
```

You now have a connection that can be used to execute SQL queries created using Swift-Kuery. View the [Kuery](https://github.com/IBM-Swift/Swift-Kuery) documentation for more information.

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE.txt).
