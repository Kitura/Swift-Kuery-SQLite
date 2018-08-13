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
import Dispatch

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
    private var inTransaction = false

    /// An indication whether there is a connection to the database.
    public var isConnected: Bool {
        return connection != nil
    }

    /// The `QueryBuilder` with SQLite specific substitutions.
    public var queryBuilder: QueryBuilder

    /// Initialiser to create a SwiftKuerySQLite instance.
    ///
    /// - Parameter location: Describes where the database is stored.
    /// - Returns: An instance of `SQLiteConnection`.
    public init(_ location: Location = .inMemory) {
        self.location = location
        self.queryBuilder = QueryBuilder(anyOnSubquerySupported: false, createAutoIncrement: SQLiteConnection.createAutoIncrement)
        queryBuilder.updateSubstitutions(
            [
                QueryBuilder.QuerySubstitutionNames.ucase : "UPPER",
                QueryBuilder.QuerySubstitutionNames.lcase : "LOWER",
                QueryBuilder.QuerySubstitutionNames.len : "LENGTH",
                // Note: SQLite's DATETIME() seems to always return UTC results,
                // whereas MySQL's NOW() will return local (system) time by de-
                // fault unless configured to use a specific timezone. Also, we
                // concatcenate " +0000" to the end of the generated formatted
                // time string since Date objects will be formatted with these
                // offsets as well and SQLite will consider
                // "2018-03-11 05:24:15" to be "smaller" than
                // "2018-03-11 05:24:15 +0000".
                QueryBuilder.QuerySubstitutionNames.now : "DATETIME() || ' +0000'",
                QueryBuilder.QuerySubstitutionNames.all : "",
                QueryBuilder.QuerySubstitutionNames.booleanTrue : "1",
                QueryBuilder.QuerySubstitutionNames.booleanFalse : "0"])
    }

    static func createAutoIncrement(_ type: String, _ primaryKey: Bool) -> String {
        if primaryKey && type == "integer" {
            return type + "AUTOINCREMENT"
        } else {
            return ""
        }
    }

    /// Initialiser with a path to where the database is stored.
    ///
    /// - Parameter filename: The path where the database is stored.
    /// - Returns: An instance of `SQLiteConnection`.
    public convenience init(filename: String) {
        self.init(.uri(filename))
    }

    deinit {
        closeConnection()
    }

    /// Create a connection pool for SQLiteConnection's.
    ///
    /// - Parameter location: Describes where the database is stored.
    /// - Returns: The `ConnectionPool` of `SQLiteConnection`.
    public static func createPool(_ location: Location = .inMemoryShared, poolOptions: ConnectionPoolOptions) -> ConnectionPool {
        let connectionGenerator: () -> Connection? = {
            let connection = SQLiteConnection(location)
            if sqlite3_open(location.description, &connection.connection) != SQLITE_OK {
                return nil
            }
            else {
                // Set the busy timeout to 200 milliseconds.
                sqlite3_busy_timeout(connection.connection, 200)
                return connection
            }
        }
        
        let connectionReleaser: (_ connection: Connection) -> () = { connection in
            connection.closeConnection()
        }
        
        return ConnectionPool(options: poolOptions, connectionGenerator: connectionGenerator, connectionReleaser: connectionReleaser)
    }

    /// Create a connection pool for SQLiteConnection's.
    ///
    /// - Parameter filename: The path where the database is stored.
    /// - Returns: The `ConnectionPool` of `SQLiteConnection`.
    public static func createPool(filename: String, poolOptions: ConnectionPoolOptions) -> ConnectionPool {
        return createPool(.uri(filename), poolOptions: poolOptions)
    }

    /// Establish a connection with the database.
    ///
    /// - Parameter onCompletion: The function to be called when the connection is established.
    public func connect(onCompletion: @escaping (QueryError?) -> ()) {
        DispatchQueue.global().async {
            let resultCode = sqlite3_open(self.location.description, &self.connection)
            var queryError: QueryError? = nil
            if resultCode != SQLITE_OK {
                let error: String? = String(validatingUTF8: sqlite3_errmsg(self.connection))
                queryError = QueryError.connection(error!)
                self.connection = nil
            }
            else {
                // Set the busy timeout to 200 milliseconds.
                sqlite3_busy_timeout(self.connection, 200)
            }
            guard queryError != nil else {
                onCompletion(nil)
                return
            }
            onCompletion(queryError)
        }
    }

    /// Establish a connection with the database.
    ///
    /// - Returns: A QueryError if the connection cannot connect, otherwise nil
    public func connectSync() -> QueryError? {
        var error: QueryError?
        let semaphore = DispatchSemaphore(value: 0)
        connect { err in
            error = err
            semaphore.signal()
        }
        semaphore.wait()
        guard let errorUnwrapped = error else {
            // Everything worked
            return nil
        }
        return errorUnwrapped
    }

    /// Return a String representation of the query.
    ///
    /// - Parameter query: The query.
    /// - Returns: A String representation of the query.
    /// - Throws: QueryError.syntaxError if query build fails.
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
        DispatchQueue.global().async {
            self.execute(query: query, parameters: [Any?](), namedParameters: [String:Any?](), onCompletion: onCompletion)
        }
    }

    /// Execute a query.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func executeSync(query: Query) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(query: query) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(sqliteQuery: raw, parameters: [Any?](), namedParameters: [String:Any?](), onCompletion: onCompletion)
        }
    }

    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func executeSync(_ raw: String) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(raw) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [Any?], onCompletion: (@escaping (QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(query: query, parameters: parameters, namedParameters: [String:Any?](), onCompletion: onCompletion)
        }
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func executeSync(query: Query, parameters: [Any?]) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(query: query, parameters: parameters) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(sqliteQuery: raw, parameters: parameters, namedParameters: [String:Any?](), onCompletion: onCompletion)
        }
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func executeSync(_ raw: String, parameters: [Any?]) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(raw, parameters: parameters) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(query: query, parameters: [Any?](), namedParameters: parameters, onCompletion: onCompletion)
        }
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func executeSync(query: Query, parameters: [String:Any?]) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(query: query, parameters: parameters) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(sqliteQuery: raw, parameters: [Any?](), namedParameters: parameters, onCompletion: onCompletion)
        }
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func executeSync(_ raw: String, parameters: [String:Any?]) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(raw, parameters: parameters) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    private func execute(query: Query, parameters: [Any?], namedParameters: [String:Any?], onCompletion: (@escaping (QueryResult) -> ())) {
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

    /// Prepare statement.
    ///
    /// - Parameter query: The query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
    public func prepareStatement(_ query: Query) throws -> PreparedStatement {
        let sqliteQuery = try query.build(queryBuilder: queryBuilder)
        return try prepareStatement(sqliteQuery)
    }

    /// Prepare statement.
    ///
    /// - Parameter raw: A String with the query to prepare statement for.
    /// - Returns: The prepared statement.
    /// - Throws: QueryError.syntaxError if query build fails, or a database error if it fails to prepare statement.
    public func prepareStatement(_ raw: String) throws -> PreparedStatement {
        let (sqliteStatement, resultCode) =  prepareSQLiteStatement(query: raw)
        guard sqliteStatement != nil, resultCode == SQLITE_OK else {
            throw(createError("Failed to prepare statement.", errorCode: resultCode))
        }
        return SQLitePreparedStatement(statement: sqliteStatement!)
    }

    /// Execute a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(preparedStatement: preparedStatement, parameters: [Any?](), namedParameters: [String:Any?](), onCompletion: onCompletion)
        }
    }

    /// Execute a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func executeSync(preparedStatement: PreparedStatement) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(preparedStatement: preparedStatement) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(preparedStatement: preparedStatement, parameters: parameters, namedParameters: [String:Any?](), onCompletion: onCompletion)
        }
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func executeSync(preparedStatement: PreparedStatement, parameters: [Any?]) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(preparedStatement: preparedStatement, parameters: parameters) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.execute(preparedStatement: preparedStatement, parameters: [Any?](), namedParameters: parameters, onCompletion: onCompletion)
        }
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func executeSync(preparedStatement: PreparedStatement, parameters: [String:Any?]) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        execute(preparedStatement: preparedStatement, parameters: parameters) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    private func execute(preparedStatement: PreparedStatement, parameters: [Any?], namedParameters: [String:Any?], onCompletion: (@escaping (QueryResult) -> ())) {
        guard let statement = preparedStatement as? SQLitePreparedStatement else {
            onCompletion(.error(QueryError.unsupported("Failed to execute unsupported prepared statement")))
            return
        }
        execute(sqliteStatement: statement.statement, parameters: parameters, namedParameters: namedParameters, finalize: false, onCompletion: onCompletion)
    }

    /// Release a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to release.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func release(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            guard let statement = preparedStatement as? SQLitePreparedStatement else {
                onCompletion(.error(QueryError.unsupported("Failed to execute unsupported prepared statement")))
                return
            }
            sqlite3_finalize(statement.statement)
            onCompletion(.successNoData)
        }
    }

    public func releaseSync(preparedStatement: PreparedStatement) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        release(preparedStatement: preparedStatement) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    private func prepareSQLiteStatement(query: String) -> (OpaquePointer?, Int32) {
        var sqliteStatement: OpaquePointer?
        var sqlTail: UnsafePointer<Int8>? = nil
        
        let resultCode = sqlite3_prepare_v2(connection, query, -1, &sqliteStatement, &sqlTail)
        return (sqliteStatement, resultCode)
    }

    private func execute(sqliteQuery: String, parameters: [Any?], namedParameters: [String:Any?], onCompletion: (@escaping (QueryResult) -> ())) {
        // Prepare SQLite statement
        let (sqliteStatement, resultCode) =  prepareSQLiteStatement(query: sqliteQuery)
        guard sqliteStatement != nil, resultCode == SQLITE_OK else {
            onCompletion(.error(createError("Failed to prepare the query statement.", errorCode: resultCode)))
            return
        }
        execute(sqliteStatement: sqliteStatement!, parameters: parameters, namedParameters: namedParameters, onCompletion: onCompletion)
    }

    private func execute(sqliteStatement: OpaquePointer, parameters: [Any?], namedParameters: [String:Any?], finalize: Bool = true, onCompletion: (@escaping (QueryResult) -> ())) {
        // Bind parameters: either numbered parameters or named parameters can be passed,
        // mixing of both types of parameters in one query is not supported
        
        // Numbered parameters
        for (i, parameter) in parameters.enumerated() {
            if let error = bind(parameter: parameter, at: Int32(i + 1), statement: sqliteStatement) {
                Utils.clear(statement: sqliteStatement, finalize: finalize)
                onCompletion(.error(error))
                return
            }
        }

        // Named parameters
        for (name, parameter) in namedParameters {
            let index = sqlite3_bind_parameter_index(sqliteStatement, "@"+name)
            if let error = bind(parameter: parameter, at: index, statement: sqliteStatement) {
                Utils.clear(statement: sqliteStatement, finalize: finalize)
                onCompletion(.error(error))
                return
            }
        }

        // Execute and get result
        let executionResultCode = sqlite3_step(sqliteStatement)
        switch executionResultCode {
        case SQLITE_DONE:
            Utils.clear(statement: sqliteStatement, finalize: finalize)
            onCompletion(.successNoData)
        case SQLITE_ROW:
            onCompletion(.resultSet(ResultSet(SQLiteResultFetcher(sqliteStatement: sqliteStatement, finalize: finalize))))
        default:
            var errorMessage = "Failed to execute the query."
            if let error = String(validatingUTF8: sqlite3_errmsg(sqliteStatement)) {
                errorMessage += " Error: \(error)"
            }
            Utils.clear(statement: sqliteStatement, finalize: finalize)
            //By offloading this to another thread the current connection context expires and connection will be returned to the pool.
            onCompletion(.error(QueryError.databaseError(errorMessage)))
        }
    }

    private func bind(parameter: Any?, at index: Int32, statement: OpaquePointer) -> QueryError? {
        var resultCode: Int32
        if parameter == nil {
           resultCode = sqlite3_bind_null(statement, index)
        }
        else {
            switch parameter {
            case let value as String:
                resultCode = sqlite3_bind_text(statement, index, value, -1, SQLITE_TRANSIENT)
            case let value as Float:
                resultCode = sqlite3_bind_double(statement, index, Double(value))
            case let value as Double:
                resultCode = sqlite3_bind_double(statement, index, value)
            case let value as Int32:
                resultCode = sqlite3_bind_int(statement, index, value)
            case let value as Int:
                resultCode = sqlite3_bind_int64(statement, index, Int64(value))
            case let value as Data:
                resultCode = sqlite3_bind_blob(statement, index, [UInt8](value), Int32(value.count), SQLITE_TRANSIENT)
            default:
                return createError("Unsupported parameter type")
            }
        }
        return (resultCode == SQLITE_OK) ? nil : createError("Failed to bind query parameter.", errorCode: resultCode)
    }

    private func createError(_ error: String, errorCode: Int32?=nil) -> QueryError {
        var errorMessage = error
        if let errorCode = errorCode, let sqliteError = String(validatingUTF8: sqlite3_errstr(errorCode)) {
            errorMessage += " SQLite error: " + sqliteError
        }
        return QueryError.databaseError(errorMessage)
    }

    /// Start a transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of start transaction command has completed.
    public func startTransaction(onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "BEGIN TRANSACTION", inTransaction: false, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
        }
    }

    /// Start a transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of start transaction command has completed.
    public func startTransactionSync() -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        startTransaction() { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Commit the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of commit transaction command has completed.
    public func commit(onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "END TRANSACTION", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
        }
    }

    /// Commit the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of commit transaction command has completed.
    public func commitSync() -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        commit() { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Rollback the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "ROLLBACK", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
        }
    }

    /// Rollback the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollbackSync() -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        rollback() { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Create a savepoint.
    ///
    /// - Parameter savepoint: The name to  be given to the created savepoint.
    /// - Parameter onCompletion: The function to be called when the execution of create savepoint command has completed.
    public func create(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to create the savepoint \(savepoint)", onCompletion: onCompletion)
        }
    }

    /// Create a savepoint.
    ///
    /// - Parameter savepoint: The name to  be given to the created savepoint.
    /// - Parameter onCompletion: The function to be called when the execution of create savepoint command has completed.
    public func createSync(savepoint: String) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        create(savepoint: savepoint) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Rollback the current transaction to the specified savepoint.
    ///
    /// - Parameter to savepoint: The name of the savepoint to rollback to.
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(to savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "ROLLBACK TO \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to rollback to the savepoint \(savepoint)", onCompletion: onCompletion)
        }
    }

    /// Rollback the current transaction to the specified savepoint.
    ///
    /// - Parameter to savepoint: The name of the savepoint to rollback to.
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollbackSync(to savepoint: String) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        rollback(to: savepoint) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    /// Release a savepoint.
    ///
    /// - Parameter savepoint: The name of the savepoint to release.
    /// - Parameter onCompletion: The function to be called when the execution of release savepoint command has completed.
    public func release(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            self.executeTransaction(command: "RELEASE SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to release the savepoint \(savepoint)", onCompletion: onCompletion)
        }
    }

    /// Release a savepoint.
    ///
    /// - Parameter savepoint: The name of the savepoint to release.
    /// - Parameter onCompletion: The function to be called when the execution of release savepoint command has completed.
    public func releaseSync(savepoint: String) -> QueryResult {
        var result: QueryResult?
        let semaphore = DispatchSemaphore(value: 0)
        release(savepoint: savepoint) { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return (.error(QueryError.noResult("No ResultSet from execute")))
        }
        return resultUnwrapped
    }

    private func executeTransaction(command: String, inTransaction: Bool, changeTransactionState: Bool, errorMessage: String, onCompletion: @escaping ((QueryResult) -> ())) {
        guard self.inTransaction == inTransaction else {
            let error = self.inTransaction ? "Transaction already exists" : "No transaction exists"
            onCompletion(.error(QueryError.transactionError(error)))
            return
        }
        
        var sqliteError: UnsafeMutablePointer<Int8>?
        let resultCode = sqlite3_exec(connection, command, nil, nil, &sqliteError)
        if resultCode != SQLITE_OK {
            var error = errorMessage
            if let sqliteError = sqliteError {
                error += ". Error\(String(cString: sqliteError))."
            }
            onCompletion(.error(QueryError.databaseError(error)))
        }
        else {
            if changeTransactionState {
                self.inTransaction = !self.inTransaction
            }

            onCompletion(.successNoData)
        }
    }
}
