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

let SQLITE_TRANSIENT = unsafeBitCast(-1, to: sqlite3_destructor_type.self)

/// An implementation of `SwiftKuery.Connection` protocol for SQLite.
/// Please see [SQLite manual](https://sqlite.org/capi3ref.html) for details.
public class SQLiteConnection: Connection {
    
    /// Stores all the results of the query
    private struct Result {
        var columnNames: [String] = []
        var results: [[Any]] = [[Any]]()
        var returnedResult: Bool = false
    }
    
    private var connection: OpaquePointer?
    private var location: Location
    
    /// The `QueryBuilder` with SQLite specific substitutions.
    public var queryBuilder: QueryBuilder
    
    /// Initialiser to create a SwiftKuerySQLite instance.
    ///
    /// - Parameter location: Describes where the database is stored.
    /// - Parameter options: not used currently
    /// - Returns: An instance of `SQLiteConnection`.
    public init(_ location: Location = .inMemory, options: [ConnectionOptions]? = nil) {
        self.location = location
        self.queryBuilder = QueryBuilder(anyOnSubquerySupported: false)
        queryBuilder.updateSubstitutions(
            [
                QueryBuilder.QuerySubstitutionNames.ucase : "UPPER",
                QueryBuilder.QuerySubstitutionNames.lcase : "LOWER",
                QueryBuilder.QuerySubstitutionNames.len : "LENGTH",
                QueryBuilder.QuerySubstitutionNames.all : "",
                QueryBuilder.QuerySubstitutionNames.booleanTrue : "1",
                QueryBuilder.QuerySubstitutionNames.booleanFalse : "0"])
    }
    
    /// Initialiser with a path to where the database is stored.
    ///
    /// - Parameter filename: The path where the database is stored.
    /// - Pparameter options: not used currently.
    /// - Returns: An instance of `SQLiteConnection`.
    public convenience init(filename: String, options: [ConnectionOptions]? = nil) {
        self.init(.uri(filename), options: options)
    }
    
    /// Establish a connection with the database.
    ///
    /// - Parameter onCompletion: The function to be called when the connection is established.
    public func connect(onCompletion: (QueryError?) -> ()) {
        let resultCode = sqlite3_open(location.description, &connection)
        var queryError: QueryError? = nil
        if resultCode != SQLITE_OK {
            let error: String? = String(validatingUTF8: sqlite3_errmsg(connection))
            queryError = QueryError.connection(error!)
        }
        else {
            // Set the busy timeout to 200 milliseconds.
            sqlite3_busy_timeout(connection, 200)
        }
        onCompletion(queryError)
    }
    
    public func descriptionOf(query: Query) throws -> String {
        return try query.build(queryBuilder: queryBuilder)
    }
    
    /// Close the connection to the database.
    public func closeConnection() {
        if let connection = connection {
            sqlite3_close(connection)
            self.connection = nil
        }
    }
    
    /// Execute a query.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        execute(query: query, parameters: [Any](), namedParameters: [String:Any](), onCompletion: onCompletion)
    }
    
    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        execute(sqliteQuery: raw, parameters: [Any](), namedParameters: [String:Any](), onCompletion: onCompletion)
    }
    
    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [Any], onCompletion: (@escaping (QueryResult) -> ())) {
        execute(query: query, parameters: parameters, namedParameters: [String:Any](), onCompletion: onCompletion)
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(sqliteQuery: raw, parameters: parameters, namedParameters: [String:Any](), onCompletion: onCompletion)
    }
    
    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(query: query, parameters: [Any](), namedParameters: parameters, onCompletion: onCompletion)
    }
    
    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(sqliteQuery: raw, parameters: [Any](), namedParameters: parameters, onCompletion: onCompletion)
    }

    private func execute(query: Query, parameters: [Any], namedParameters: [String:Any], onCompletion: (@escaping (QueryResult) -> ())) {
        do {
            let sqliteQuery = try query.build(queryBuilder: queryBuilder)
            execute(sqliteQuery: sqliteQuery, parameters: parameters, namedParameters: namedParameters, onCompletion: onCompletion)
        }
        catch QueryError.syntaxError(let error) {
            onCompletion(.error(QueryError.syntaxError(error)))
        }
        catch {
            onCompletion(.error(QueryError.syntaxError("Failed to build the query")))
        }
    }
    
    private func bind(parameter: Any, at index: Int32, statement: OpaquePointer) -> String? {
        var result: Int32
        switch parameter {
        case let value as String:
            result = sqlite3_bind_text(statement, Int32(index), value, -1, SQLITE_TRANSIENT)
        case let value as Float:
            result = sqlite3_bind_double(statement, Int32(index), Double(value))
        case let value as Double:
            result = sqlite3_bind_double(statement, Int32(index), value)
        case let value as Int:
            result = sqlite3_bind_int64(statement, Int32(index), Int64(value))
        case let value as Data:
            result = sqlite3_bind_blob(statement, Int32(index), [UInt8](value), Int32(value.count), SQLITE_TRANSIENT)
        default:
            return "Unsupported parameter type"
        }
        guard result == SQLITE_OK else {
            return "Failed to bind query parameter"
        }
            return nil
    }
    
    private func execute(sqliteQuery: String, parameters: [Any], namedParameters: [String:Any], onCompletion: (@escaping (QueryResult) -> ())) {
        do {
            var sqliteStatement: OpaquePointer?
            var sqlTail: UnsafePointer<Int8>? = nil
            
            // Prepare SQLite statement
            guard sqlite3_prepare_v2(connection, sqliteQuery, -1, &sqliteStatement, &sqlTail) == SQLITE_OK else {
                onCompletion(.error(QueryError.databaseError("Failed to prepare the query statement")))
                return
            }
            
            // Bind parameters: either numbered parameters or named parameters can be passed,
            // mixing of both types of parameters in one query is not supported
            
            // Numbered parameters
            for (i, parameter) in parameters.enumerated() {
                if let error = bind(parameter: parameter, at: i+1, statement: sqliteStatement!) {
                    onCompletion(.error(QueryError.databaseError(error)))
                    return
                }
            }
            
            // Named parameters
            for (name, parameter) in namedParameters {
                let index = sqlite3_bind_parameter_index(sqliteStatement, name)
                if let error = bind(parameter: parameter, at: index, statement: sqliteStatement!) {
                    onCompletion(.error(QueryError.databaseError(error)))
                    return
                }
            }
            
            // Execute and get result
            // TODO: Handle SQLITE_BUSY
            let result = sqlite3_step(sqliteStatement)
            switch result {
            case SQLITE_DONE:
                sqlite3_finalize(sqliteStatement)
                onCompletion(.successNoData)
            case SQLITE_ROW:
                onCompletion(.resultSet(ResultSet(SQLiteResultFetcher(sqliteStatement: sqliteStatement!))))
            default:
                let error = String(validatingUTF8: sqlite3_errmsg(sqliteStatement!))
                sqlite3_finalize(sqliteStatement)
                onCompletion(.error(QueryError.databaseError("Failed to execute the query. Error: \(error)")))
            }
        }
    }
}
