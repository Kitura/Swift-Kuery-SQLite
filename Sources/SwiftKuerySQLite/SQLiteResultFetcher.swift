/**
 Copyright IBM Corporation 2016, 2017
 
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at
 
 http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import SwiftKuery
#if os(Linux)
    import CSQLiteLinux
#else
    import CSQLiteDarwin
#endif
import Foundation

// MARK: SQLiteResultFetcher

/// An implementation of query result fetcher.
public class SQLiteResultFetcher: ResultFetcher {
    
    private let titles: [String]
    private var firstRow: [Any?]?
    private let sqliteStatement: OpaquePointer
    private let numberOfColumns: Int32
    private var hasMoreRows = true
    private var finalize: Bool
    
    init(sqliteStatement: OpaquePointer, finalize: Bool) {
        self.finalize = finalize
        numberOfColumns = sqlite3_column_count(sqliteStatement)
        var columnNames = [String]()
        for i in 0..<numberOfColumns {
            if let name = sqlite3_column_name(sqliteStatement, Int32(i)) {
                columnNames.append(String(cString: name))
            }
        }
        titles = columnNames
        
        self.sqliteStatement = sqliteStatement
        firstRow = buildRow()
    }
    
    deinit {
        done()
    }

    /// Indicate no further calls will be made to this ResultFetcher allowing the connection in use to be released.
    ///
    public func done() {
        if hasMoreRows {
            Utils.clear(statement: sqliteStatement, finalize: finalize)
        }
    }
    
    /// Fetch the next row of the query result. This function is non-blocking.
    ///
    /// - Parameter callback: A callback to call when the next row of the query result is ready.
    public func fetchNext(callback: @escaping (([Any?]?, Error?)) -> ()) {
        DispatchQueue.global().async {
            if let row = self.firstRow {
                self.firstRow = nil
                return callback((row, nil))
            }
            guard self.hasMoreRows else {
                return callback((nil,nil))
            }
            let result = sqlite3_step(self.sqliteStatement)
            switch result {
            case SQLITE_ROW:
                return callback((self.buildRow(), nil))
            default:
                self.hasMoreRows = false
                Utils.clear(statement: self.sqliteStatement, finalize: self.finalize)
                return callback((nil, nil))
            }
        }
    }
    
    /// Fetch the titles of the query result. This function is blocking.
    ///
    /// - Parameter callback: A callback to call passing an array of column titles of type String.
    public func fetchTitles(callback: @escaping (([String]?, Error?)) -> ()) {
        // As titles is already populated when this ResultFetcher is initialised we can simply return without blocking or offload.
        callback((titles, nil))
    }
    
    private func buildRow() -> [Any?] {
        var row = [Any?]()
        for i in 0..<numberOfColumns {
            var value: Any?
            switch sqlite3_column_type(sqliteStatement, i) {
            case SQLITE_INTEGER:
                value = sqlite3_column_int64(sqliteStatement, i)
            case SQLITE_FLOAT:
                value = sqlite3_column_double(sqliteStatement, i)
            case SQLITE_TEXT:
                value = String(cString: sqlite3_column_text(sqliteStatement, i))
            case SQLITE_BLOB:
                let count = sqlite3_column_bytes(sqliteStatement, i)
                if let bytes = sqlite3_column_blob(sqliteStatement, i) {
                    value = Data(bytes: bytes, count: Int(count))
                }
                else {
                    value = nil
                }
            case SQLITE_NULL:
                value = nil
            default:
                value = nil
            }
            row.append(value)
        }
        
       return row
    }
}

