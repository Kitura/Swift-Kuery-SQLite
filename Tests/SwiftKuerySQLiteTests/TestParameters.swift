/**
 Copyright IBM Corporation 2017
 
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

import XCTest
import SwiftKuery

@testable import SwiftKuerySQLite

#if os(Linux)
let tableParameters = "tableParametersLinux"
let tablePreparedStatements = "tablePreparedStatementsLinux"
#else
let tableParameters = "tableParametersOSX"
let tablePreparedStatements = "tablePreparedStatementsOSX"
#endif

class TestParameters: XCTestCase {
    
    static var allTests: [(String, (TestParameters) -> () throws -> Void)] {
        return [
            ("testParameters", testParameters),
            ("testPreparedStatements", testPreparedStatements),
        ]
    }
    
    class MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        let tableName = tableParameters
    }
    
    func testParameters() {
        let t = MyTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { result in

                    executeRawQuery("CREATE TABLE \"" +  t.tableName + "\" (a varchar(40), b integer)", connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")

                        let i1 = Insert(into: t, rows: [[Parameter(), 10], ["apricot", Parameter()], [Parameter(), Parameter()]])
                        executeQueryWithParameters(query: i1, connection: connection, parameters: "apple", 3, "banana", -8) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows")
                                XCTAssertEqual(rows![0][0]! as! String, "apple", "Wrong value in row 0 column 0")
                                XCTAssertEqual(rows![1][0]! as! String, "apricot", "Wrong value in row 1 column 0")
                                XCTAssertEqual(rows![2][0]! as! String, "banana", "Wrong value in row 2 column 0")
                                XCTAssertEqual(rows![0][1]! as! Int32, 10, "Wrong value in row 0 column 1")
                                XCTAssertEqual(rows![1][1]! as! Int32, 3, "Wrong value in row 1 column 1")
                                XCTAssertEqual(rows![2][1]! as! Int32, -8, "Wrong value in row 2 column 1")

                                let u1 = Update(t, set: [(t.a, Parameter()), (t.b, Parameter())], where: t.a == "banana")
                                executeQueryWithParameters(query: u1, connection: connection, parameters: "peach", 2) { result, rows in
                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                    executeQuery(query: s1, connection: connection) { result, rows in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                                        XCTAssertEqual(rows![2][0]! as! String, "peach", "Wrong value in row 2 column 0")
                                        XCTAssertEqual(rows![2][1]! as! Int32, 2, "Wrong value in row 2 column 1")

                                        let raw = "UPDATE \"" + t.tableName + "\" SET a = 'banana', b = $1 WHERE a = $2"
                                        executeRawQueryWithParameters(raw, connection: connection, parameters: 4, "peach") { result, rows in
                                            XCTAssertEqual(result.success, true, "UPDATE failed")
                                            XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                            executeQuery(query: s1, connection: connection) { result, rows in
                                                XCTAssertEqual(result.success, true, "SELECT failed")
                                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                                XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                                                XCTAssertEqual(rows![2][0]! as! String, "banana", "Wrong value in row 2 column 0")
                                                XCTAssertEqual(rows![2][1]! as! Int32, 4, "Wrong value in row 2 column 1")

                                                let u2 = Update(t, set: [(t.a, Parameter("first")), (t.b, Parameter("second"))], where: t.a == "banana")
                                                executeQueryWithNamedParameters(query: u2, connection: connection, parameters: ["second":2, "first":"peach"]) { result, rows in
                                                    XCTAssertEqual(result.success, true, "UPDATE failed")
                                                    XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError!)")

                                                    executeQuery(query: s1, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                                        XCTAssertEqual(rows!.count, 3, "SELECT returned wrong number of rows: \(rows!.count) instead of 3")
                                                        XCTAssertEqual(rows![2][0]! as! String, "peach", "Wrong value in row 2 column 0")
                                                        XCTAssertEqual(rows![2][1]! as! Int32, 2, "Wrong value in row 2 column 1")

                                                        let s2 = Select(from: t).where(t.a != Parameter())
                                                        executeQueryWithParameters(query: s2, connection: connection, parameters: nil) { result, rows in
                                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                                            expectation.fulfill()
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
    
    class PreparedTable: Table {
        let a = Column("a", Varchar.self, length: 40)
        let b = Column("b", Int32.self)
        
        let tableName = tablePreparedStatements
    }
    
    func testPreparedStatements() {
        let t = PreparedTable()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            pool.getConnection { connection, error in
                guard let connection = connection else {
                    XCTFail("Failed to get connection")
                    return
                }
                cleanUp(table: t.tableName, connection: connection) { result in

                    t.create(connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError)

                        let i1 = Insert(into: t, rows: [[Parameter(), 10], ["banana", Parameter()], [Parameter(), Parameter()]])
                        connection.prepareStatement(i1) { result in
                            guard let preparedInsert = result.asPreparedStatement else {
                                if let error = result.asError {
                                    XCTFail("Unable to prepare statement preparedInsert: \(error.localizedDescription)")
                                }
                                XCTFail("Unable to prepare statement preparedInsert")
                                expectation.fulfill()
                                return
                            }

                            connection.execute(preparedStatement: preparedInsert, parameters: ["apple", 3, "banana", -8]) { result in
                                XCTAssertEqual(result.success, true, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                                let s1 = Select(from: t).where(t.a == Parameter("fruit"))
                                connection.prepareStatement(s1) { result in
                                    guard let preparedSelect = result.asPreparedStatement else {
                                        if let error = result.asError {
                                            XCTFail("Unable to prepare statement preparedSelect: \(error.localizedDescription)")
                                        }
                                        XCTFail("Unable to prepare statement preparedSelect")
                                        expectation.fulfill()
                                        return
                                    }

                                    connection.execute(preparedStatement: preparedSelect, parameters: ["fruit":"apple"]) { result in
                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                        let rows = result.asRows
                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                        XCTAssertEqual(rows!.count, 1, "Wrong number of rows")

                                        connection.execute(preparedStatement: preparedSelect, parameters: ["fruit":"banana"]) { result in
                                            XCTAssertEqual(result.success, true, "SELECT failed")
                                            let rows = result.asRows
                                            XCTAssertNotNil(rows, "SELECT returned no rows")
                                            XCTAssertEqual(rows!.count, 2, "Wrong number of rows")

                                            let s2 = "SELECT * FROM " + t.tableName
                                            connection.prepareStatement(s2) { result in
                                                guard let preparedSelect2 = result.asPreparedStatement else {
                                                    if let error = result.asError {
                                                        XCTFail("Unable to prepare statement preparedSelect2: \(error.localizedDescription)")
                                                    }
                                                    XCTFail("Unable to prepare statement preparedSelect2")
                                                    expectation.fulfill()
                                                    return
                                                }

                                                connection.execute(preparedStatement: preparedSelect2) { result in
                                                    XCTAssertEqual(result.success, true, "SELECT failed")
                                                    let rows = result.asRows
                                                    XCTAssertNotNil(rows, "SELECT returned no rows")
                                                    XCTAssertEqual(rows!.count, 3, "Wrong number of rows")

                                                    connection.release(preparedStatement: preparedInsert) { result in
                                                        XCTAssertNil(result.asError, "Error in release statement: \(result.asError!)")

                                                        connection.release(preparedStatement: preparedSelect) { result in
                                                            XCTAssertNil(result.asError, "Error in release statement: \(result.asError!)")

                                                            connection.release(preparedStatement: preparedSelect2) { result in
                                                                XCTAssertNil(result.asError, "Error in release statement: \(result.asError!)")
                                                                expectation.fulfill()
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }
}
