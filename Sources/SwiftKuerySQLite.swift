/**
 Copyright IBM Corporation 2016
 
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
    import CSQLiteMac
#endif

import Foundation

public class SwiftKuerySQLite: Connection {

    private var connection: OpaquePointer? = nil///???
    private var location: Location
    public var queryBuilder: QueryBuilder
    
    /// Init
    ///
    /// - parameter location: Describes where the db is stored
    /// - parameter options:  not used currently
    ///
    /// - returns:
    public init(_ location: Location = .inMemory, options: [ConnectionOptions]?) {
        self.location = location
        self.queryBuilder = QueryBuilder()
        queryBuilder.updateNames([QueryBuilder.QueryNames.ascd : "ASC", QueryBuilder.QueryNames.ucase : "UPPER", QueryBuilder.QueryNames.lcase : "LOWER", QueryBuilder.QueryNames.len : "LENGTH"])
    }
    
    
    /// Init with the file name
    ///
    /// - parameter filename: The path where the db is stored
    /// - parameter options:  not used currently
    ///
    /// - returns:
    public convenience init(filename: String, options: [ConnectionOptions]?) {
        self.init(.uri(filename), options: options)
    }

    
    /// Connects to the db
    ///
    /// - parameter onCompletion: callback returning an error or a 0 if successful
    public func connect(onCompletion: (QueryError?) -> ()) {
        let resultCode = sqlite3_open(location.description, &connection)
        var queryError: QueryError? = nil
        if resultCode != SQLITE_OK {
            let error: String? = String(validatingUTF8: sqlite3_errmsg(connection))
            queryError = QueryError.connection(error!)
        }
        onCompletion(queryError)
    }
    
    
    public func descriptionOf(query: Query) -> String {
        return query.build(queryBuilder: queryBuilder)
    }
    
    
    /// Close the connection to the db
    public func closeConnection() {
        if let connection = connection {
            sqlite3_close(connection)
            self.connection = nil
        }
    }
    
    public func execute(query: Query, parameters: Any..., onCompletion: (@escaping (QueryResult) -> ())) {
        
    }

    
    /// Executes a query
    ///
    /// - parameter query:        the query
    /// - parameter onCompletion: The result
    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        let sqliteQuery = query.build(queryBuilder: queryBuilder)
        executeQuery(query: sqliteQuery, onCompletion: onCompletion)
    }
    
    
    /// Executes a raw query
    ///
    /// - parameter raw:          The full raw query
    /// - parameter onCompletion: The result
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeQuery(query: raw, onCompletion: onCompletion)
    }
    
    
    /// Actually executes the query
    ///
    /// - parameter query:        the query
    /// - parameter onCompletion: the result
    private func executeQuery(query: String, onCompletion: @escaping ((QueryResult) -> ())) {

        var errmsg: UnsafeMutablePointer<Int8>?
        var result = Result()
        
        //This is where we bridge to the c code
        //connection - the OpaquePointer to the db
        //query - the query to execute
        //the callback - if there are any results to be returned this will run otherwise it will skip
        //               calling the callback
        //result - stores the result of the callback, if there are any
        //errmsg - the error message if something goes wrong
        let resultCode = sqlite3_exec(connection, query, {
            (data, cols, colText, colName) -> Int32 in
                let values = data?.assumingMemoryBound(to: Result.self)
                let intcols: Int = Int(cols)

                //only add the columns once
                if (values?.pointee.columnNames.count)! < intcols {
                    for j in 0..<intcols {
                        values?.pointee.columnNames.append(String(cString: (colName?[j])!))
                    }
                }

                //adds the row
                var singleRow = [Any]()
                for i in 0..<intcols {
                    singleRow.append(String(cString: (colText?[i])!))
                }
                values?.pointee.results.append(singleRow)
                values?.pointee.returnedResult = true

                //has to return 0 for a successful execute
                return 0
            }, &result, &errmsg)

        
        if resultCode == SQLITE_OK {
            if result.returnedResult {
                onCompletion(.rows(titles: result.columnNames, rows: result.results))
            } else {
                onCompletion(.successNoData)
            }
        } else if let errmsg = errmsg {
            onCompletion(.error(QueryError.databaseError(String(cString: errmsg))))
        }
    }
}

// struct to store the results of the callback when executing the callback
private struct Result {
    var columnNames: [String] = []
    var results: [[Any]] = [[Any]]()
    var returnedResult: Bool = false
}

// enum for the 3 differnt types of where the db can be
public enum Location {
    case inMemory
    case temporary
    case uri(String)
}

extension Location: CustomStringConvertible {
    public var description: String {
        switch self {
        case .inMemory:
            return ":memory:"
        case .temporary:
            return ""
        case .uri(let uri):
            return uri
        }
    }
}

public enum ConnectionOptions {
    case options(String)
    case databaseName(String)
    case userName(String)
    case password(String)
    case connectionTimeout(Int)
    case readOnly(Bool)
}
