/**
 Copyright IBM Corporation 2016, 2017, 2018, 2019
 
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
        self.queryBuilder = QueryBuilder(anyOnSubquerySupported: false, columnBuilder: SQLiteColumnBuilder())
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
                QueryBuilder.QuerySubstitutionNames.mid : "SUBSTR",
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
    public func connect(onCompletion: @escaping (QueryResult) -> ()) {
        DispatchQueue.global().async {
            let resultCode = sqlite3_open(self.location.description, &self.connection)
            if resultCode != SQLITE_OK {
                let error: String? = String(validatingUTF8: sqlite3_errmsg(self.connection))
                self.connection = nil
                return self.runCompletionHandler(.error(QueryError.connection(error!)), onCompletion: onCompletion)
            }
            // Set the busy timeout to 200 milliseconds.
            sqlite3_busy_timeout(self.connection, 200)
            return self.runCompletionHandler(.successNoData, onCompletion: onCompletion)
        }
    }

    /// Establish a connection with the database.
    ///
    /// - Returns: A QueryError if the connection cannot connect, otherwise nil
    public func connectSync() -> QueryResult {
        var result: QueryResult? = nil
        let semaphore = DispatchSemaphore(value: 0)
        connect() { res in
            result = res
            semaphore.signal()
        }
        semaphore.wait()
        guard let resultUnwrapped = result else {
            return .error(QueryError.connection("ConnectSync unexpetedly return a nil QueryResult"))
        }
        return resultUnwrapped
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
        execute(query: query, parameters: [Any?](), namedParameters: [String:Any?](), onCompletion: onCompletion)
    }

    /// Execute a raw query.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        execute(sqliteQuery: raw, parameters: [Any?](), namedParameters: [String:Any?](), onCompletion: onCompletion)
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [Any?], onCompletion: (@escaping (QueryResult) -> ())) {
        execute(query: query, parameters: parameters, namedParameters: [String:Any?](), onCompletion: onCompletion)
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(sqliteQuery: raw, parameters: parameters, namedParameters: [String:Any?](), onCompletion: onCompletion)
    }

    /// Execute a query with parameters.
    ///
    /// - Parameter query: The query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(query: Query, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(query: query, parameters: [Any?](), namedParameters: parameters, onCompletion: onCompletion)
    }

    /// Execute a raw query with parameters.
    ///
    /// - Parameter query: A String with the query to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution of the query has completed.
    public func execute(_ raw: String, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(sqliteQuery: raw, parameters: [Any?](), namedParameters: parameters, onCompletion: onCompletion)
    }

    private func execute(query: Query, parameters: [Any?], namedParameters: [String:Any?], onCompletion: (@escaping (QueryResult) -> ())) {
        do {
            let sqliteQuery = try query.build(queryBuilder: queryBuilder)

            execute(sqliteQuery: sqliteQuery, parameters: parameters, namedParameters: namedParameters) { queryResult in
                // Only recover last insert ID if execute was succesful
                if let insertQuery = query as? Insert, insertQuery.returnID, queryResult.success {
                    guard let idColumn = insertQuery.table.columns.first(where: {$0.isPrimaryKey && $0.autoIncrement}) else {
                        onCompletion(.error(QueryError.syntaxError("Failed to find the ID column name")))
                        return
                    }
                    self.execute(sqliteQuery: "Select last_insert_rowid() as \(idColumn.name);", parameters: [Any?](), namedParameters: [:]) { queryResult in
                        onCompletion(queryResult)
                    }
                } else {
                    onCompletion(queryResult)
                }
            }
        }
        catch QueryError.syntaxError(let error) {
            return runCompletionHandler(.error(QueryError.syntaxError(error)), onCompletion: onCompletion)
        }
        catch {
            return runCompletionHandler(.error(QueryError.syntaxError("Failed to build the query")), onCompletion: onCompletion)
        }
    }

    /// Prepare statement.
    ///
    /// - Parameter query: The query to prepare statement for.
    /// - Parameter onCompletion: The function to be called when the statementhas been prepared.
    public func prepareStatement(_ query: Query, onCompletion: @escaping ((QueryResult) -> ())) {
        var sqliteQuery: String
        do {
            sqliteQuery = try query.build(queryBuilder: queryBuilder)
        } catch let error {
            return runCompletionHandler(.error(QueryError.syntaxError("Unable to prepare statement: \(error.localizedDescription)")), onCompletion: onCompletion)
        }
        prepareStatement(sqliteQuery, onCompletion: onCompletion)
    }

    /// Prepare statement.
    ///
    /// - Parameter raw: A String with the query to prepare statement for.
    /// - Parameter onCompletion: The function to be called when the statementhas been prepared.
    public func prepareStatement(_ raw: String, onCompletion: @escaping ((QueryResult) -> ())) {
        DispatchQueue.global().async {
            var sqliteStatement: OpaquePointer? = nil
            var sqlTail: UnsafePointer<Int8>? = nil

            let resultCode = sqlite3_prepare_v2(self.connection, raw, -1, &sqliteStatement, &sqlTail)
            guard let unwrappedSqliteStatement = sqliteStatement, resultCode == SQLITE_OK else {
                return self.runCompletionHandler(.error(QueryError.databaseError("Unable to prepare statement, error code: \(resultCode)")), onCompletion: onCompletion)
            }
            return self.runCompletionHandler(.success(SQLitePreparedStatement(statement: unwrappedSqliteStatement)), onCompletion: onCompletion)
        }
    }

    /// Execute a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        execute(preparedStatement: preparedStatement, parameters: [Any?](), namedParameters: [String:Any?](), onCompletion: onCompletion)
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: An array of the parameters.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(preparedStatement: preparedStatement, parameters: parameters, namedParameters: [String:Any?](), onCompletion: onCompletion)
    }

    /// Execute a prepared statement with parameters.
    ///
    /// - Parameter preparedStatement: The prepared statement to execute.
    /// - Parameter parameters: A dictionary of the parameters with parameter names as the keys.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func execute(preparedStatement: PreparedStatement, parameters: [String:Any?], onCompletion: @escaping ((QueryResult) -> ())) {
        execute(preparedStatement: preparedStatement, parameters: [Any?](), namedParameters: parameters, onCompletion: onCompletion)
    }

    private func execute(preparedStatement: PreparedStatement, parameters: [Any?], namedParameters: [String:Any?], onCompletion: (@escaping (QueryResult) -> ())) {
        guard let statement = preparedStatement as? SQLitePreparedStatement else {
            return runCompletionHandler(.error(QueryError.unsupported("Failed to execute unsupported prepared statement")), onCompletion: onCompletion)
        }
        execute(sqliteStatement: statement.statement, parameters: parameters, namedParameters: namedParameters, finalize: false, onCompletion: onCompletion)
    }

    /// Release a prepared statement.
    ///
    /// - Parameter preparedStatement: The prepared statement to release.
    /// - Parameter onCompletion: The function to be called when the execution has completed.
    public func release(preparedStatement: PreparedStatement, onCompletion: @escaping ((QueryResult) -> ())) {
        guard let statement = preparedStatement as? SQLitePreparedStatement else {
            return self.runCompletionHandler(.error(QueryError.unsupported("Failed to execute unsupported prepared statement")), onCompletion: onCompletion)
        }
        DispatchQueue.global().async {
            sqlite3_finalize(statement.statement)
            return self.runCompletionHandler(.successNoData, onCompletion: onCompletion)
        }
    }

    private func execute(sqliteQuery: String, parameters: [Any?], namedParameters: [String:Any?], onCompletion: (@escaping (QueryResult) -> ())) {
        prepareStatement(sqliteQuery) { result in
            guard let statement = result.asPreparedStatement else {
                if let error = result.asError {
                    return self.runCompletionHandler(.error(QueryError.databaseError("\(error.localizedDescription)")), onCompletion: onCompletion)
                }
                return self.runCompletionHandler(.error(QueryError.databaseError("Unable to prepare statement")), onCompletion: onCompletion)
            }
            let sqliteStatement = statement as! SQLitePreparedStatement
            return self.execute(sqliteStatement: sqliteStatement.statement, parameters: parameters, namedParameters: namedParameters, onCompletion: onCompletion)
        }
    }

    private func execute(sqliteStatement: OpaquePointer, parameters: [Any?], namedParameters: [String:Any?], finalize: Bool = true, onCompletion: (@escaping (QueryResult) -> ())) {
        DispatchQueue.global().async {
            // Bind parameters: either numbered parameters or named parameters can be passed,
            // mixing of both types of parameters in one query is not supported

            // Numbered parameters
            for (i, parameter) in parameters.enumerated() {
                if let error = self.bind(parameter: parameter, at: Int32(i + 1), statement: sqliteStatement) {
                    Utils.clear(statement: sqliteStatement, finalize: finalize)
                    return self.runCompletionHandler(.error(error), onCompletion: onCompletion)
                }
            }

            // Named parameters
            for (name, parameter) in namedParameters {
                let index = sqlite3_bind_parameter_index(sqliteStatement, "@"+name)
                if let error = self.bind(parameter: parameter, at: index, statement: sqliteStatement) {
                    Utils.clear(statement: sqliteStatement, finalize: finalize)
                    return self.runCompletionHandler(.error(error), onCompletion: onCompletion)
                }
            }

            // Execute and get result
            let executionResultCode = sqlite3_step(sqliteStatement)
            switch executionResultCode {
            case SQLITE_DONE:
                Utils.clear(statement: sqliteStatement, finalize: finalize)
                return self.runCompletionHandler(.successNoData, onCompletion: onCompletion)
            case SQLITE_ROW:
                return self.runCompletionHandler(.resultSet(ResultSet(SQLiteResultFetcher(sqliteStatement: sqliteStatement, finalize: finalize), connection: self)), onCompletion: onCompletion)
            default:
                var errorMessage = "Failed to execute the query."
                if let error = String(validatingUTF8: sqlite3_errmsg(sqliteStatement)) {
                    errorMessage += " Error: \(error)"
                }
                Utils.clear(statement: sqliteStatement, finalize: finalize)
                return self.runCompletionHandler(.error(QueryError.databaseError(errorMessage)), onCompletion: onCompletion)
            }
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
        executeTransaction(command: "BEGIN TRANSACTION", inTransaction: false, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
    }

    /// Commit the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of commit transaction command has completed.
    public func commit(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "END TRANSACTION", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
    }

    /// Rollback the current transaction.
    ///
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "ROLLBACK", inTransaction: true, changeTransactionState: true, errorMessage: "Failed to rollback the transaction", onCompletion: onCompletion)
    }

    /// Create a savepoint.
    ///
    /// - Parameter savepoint: The name to  be given to the created savepoint.
    /// - Parameter onCompletion: The function to be called when the execution of create savepoint command has completed.
    public func create(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to create the savepoint \(savepoint)", onCompletion: onCompletion)
    }

    /// Rollback the current transaction to the specified savepoint.
    ///
    /// - Parameter to savepoint: The name of the savepoint to rollback to.
    /// - Parameter onCompletion: The function to be called when the execution of rolback transaction command has completed.
    public func rollback(to savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "ROLLBACK TO \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to rollback to the savepoint \(savepoint)", onCompletion: onCompletion)
    }

    /// Release a savepoint.
    ///
    /// - Parameter savepoint: The name of the savepoint to release.
    /// - Parameter onCompletion: The function to be called when the execution of release savepoint command has completed.
    public func release(savepoint: String, onCompletion: @escaping ((QueryResult) -> ())) {
        executeTransaction(command: "RELEASE SAVEPOINT \(savepoint)", inTransaction: true, changeTransactionState: false, errorMessage: "Failed to release the savepoint \(savepoint)", onCompletion: onCompletion)
    }

    private func executeTransaction(command: String, inTransaction: Bool, changeTransactionState: Bool, errorMessage: String, onCompletion: @escaping ((QueryResult) -> ())) {
        guard self.inTransaction == inTransaction else {
            let error = self.inTransaction ? "Transaction already exists" : "No transaction exists"
            return runCompletionHandler(.error(QueryError.transactionError(error)), onCompletion: onCompletion)
        }
        
        DispatchQueue.global().async {
            var sqliteError: UnsafeMutablePointer<Int8>?
            let resultCode = sqlite3_exec(self.connection, command, nil, nil, &sqliteError)
            if resultCode != SQLITE_OK {
                var error = errorMessage
                if let sqliteError = sqliteError {
                    error += ". Error\(String(cString: sqliteError))."
                }
                return self.runCompletionHandler(.error(QueryError.databaseError(error)), onCompletion: onCompletion)
            }
            else {
                if changeTransactionState {
                    self.inTransaction = !self.inTransaction
                }
                return self.runCompletionHandler(.successNoData, onCompletion: onCompletion)
            }
        }
    }
}

class SQLiteColumnBuilder: ColumnCreator {
    func buildColumn(for column: Column, using queryBuilder: QueryBuilder) -> String? {
        guard let type = column.type else {
            return nil
        }

        var result = column.name
        let identifierQuoteCharacter = queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.identifierQuoteCharacter.rawValue]
        if !result.hasPrefix(identifierQuoteCharacter) {
            result = identifierQuoteCharacter + result + identifierQuoteCharacter + " "
        }

        var typeString = type.create(queryBuilder: queryBuilder)
        if isInvalidIntegerType(typeString, queryBuilder) {
            return nil
        }
        if let length = column.length {
            typeString += "(\(length))"
        }
        if column.autoIncrement {
            // SQLite only allows you to define columns as integer primary key autoincrement but as SQLite is not strongly typed the resulting column is actually an 8 byte integer. As such we require the column to be bigint (Int64) to avoid truncation of values greater than INT32_MAX when reading from the database.
            if column.isPrimaryKey && typeString == "bigint" {
                result += "integer" + " PRIMARY KEY" + " AUTOINCREMENT"
            } else {
                return nil
            }
        } else {
            result += typeString
        }

        if column.isPrimaryKey && !column.autoIncrement {
            result += " PRIMARY KEY"
        }
        if column.isNotNullable {
            result += " NOT NULL"
        }
        if column.isUnique {
            result += " UNIQUE"
        }
        if let defaultValue = getDefaultValue(for: column, queryBuilder: queryBuilder) {
            result += " DEFAULT " + defaultValue
        }
        if let checkExpression = column.checkExpression {
            result += checkExpression.contains(column.name) ? " CHECK (" + checkExpression.replacingOccurrences(of: column.name, with: "\"\(column.name)\"") + ")" : " CHECK (" + checkExpression + ")"
        }
        if let collate = column.collate {
            result += " COLLATE \"" + collate + "\""
        }
        return result
    }

    func isInvalidIntegerType(_ typeString: String, _ queryBuilder: QueryBuilder) -> Bool {
        // Invalid integer types are Int16 and Int32
        //Int16 - smallint
        //Int32 - queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.int32.rawValue]
        if ((typeString == "smallint") || (typeString == queryBuilder.substitutions[QueryBuilder.QuerySubstitutionNames.int32.rawValue])) {
            return true
        }
        return false
    }
}
