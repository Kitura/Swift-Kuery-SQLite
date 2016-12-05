# Swift-Kuery-SQLite
SQLite plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework.

![macOS](https://img.shields.io/badge/os-macOS-green.svg?style=flat)
![Linux](https://img.shields.io/badge/os-linux-green.svg?style=flat)
![Apache 2](https://img.shields.io/badge/license-Apache2-blue.svg?style=flat)

## Summary
[SQLite](https://sqlite.org/) plugin for the [Swift-Kuery](https://github.com/IBM-Swift/Swift-Kuery) framework. It enables you to use Swift-Kuery to manipulate data in a SQLite database.

## SQLite installation

To use Swift-Kuery-SQLite you must have the appropriate PostgreSQL C-language client installed.

### macOS
```
$ brew install sqlite
```

### Linux
```
$ sudo apt-get install sqlite3 libsqlite3-dev
```

## Using Swift-Kuery-SQLite

First create an instance of `Swift-Kuery-SQLite` by calling:

```swift
let connection = SQLiteConnection(filename: "myDB.db")
```

To establish a connection call:

```swift
SQLiteConnection.connect(onCompletion: (QueryError?) -> ())
```

You now have a connection that can be used to execute SQL queries created using Swift-Kuery. View the [Kuery](https://github.com/IBM-Swift/Swift-Kuery) documentation for more information.

## License
This library is licensed under Apache 2.0. Full license text is available in [LICENSE](LICENSE.txt).
