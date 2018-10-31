/**
 Copyright IBM Corporation 2017, 2018
 
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
import Foundation

@testable import SwiftKuerySQLite

#if os(Linux)
let tableNameSuffix = "Linux"
#else
let tableNameSuffix = "OSX"
#endif

class TestSchema: XCTestCase {
    
    static var allTests: [(String, (TestSchema) -> () throws -> Void)] {
        return [
            ("testCreateTable", testCreateTable),
            ("testForeignKeys", testForeignKeys),
            ("testPrimaryKeys", testPrimaryKeys),
            ("testTypes", testTypes),
            ("testAutoIncrement", testAutoIncrement),
            ("testInt64", testInt64),
        ]
    }
    
    class MyTable: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi", collate: "BINARY")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95, check: "c > 0")
        
        let tableName = "MyTable" + tableNameSuffix
    }
    
    class MyNewTable: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)
        
        let tableName = "MyNewTable" + tableNameSuffix
    }

    func testCreateTable() {
        let t = MyTable()
        let tNew = MyNewTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t.tableName, connection: connection) { result in
                cleanUp(table: tNew.tableName, connection: connection) { result in
                    
                    t.create(connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        let i1 = Insert(into: t, valueTuples: (t.a, "apple"), (t.b, 5))
                        executeQuery(query: i1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                            
                            let s1 = Select(from: t)
                            executeQuery(query: s1, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")
                                
                                let resultSet = result.asResultSet!
                                XCTAssertEqual(resultSet.titles.count, 3, "SELECT returned wrong number of titles")
                                XCTAssertEqual(resultSet.titles[0], "a", "Wrong column name for column 0")
                                XCTAssertEqual(resultSet.titles[1], "b", "Wrong column name for column 1")
                                XCTAssertEqual(resultSet.titles[2], "c", "Wrong column name for column 2")
                                
                                XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                                XCTAssertEqual(rows![0].count, 3, "SELECT returned wrong number of columns")
                                XCTAssertEqual(rows![0][0]! as? String, "apple", "Wrong value in row 0 column 0")
                                XCTAssertEqual(rows![0][1]! as? sqliteInt, 5, "Wrong value in row 0 column 1")
                                XCTAssertEqual(rows![0][2]! as? Double, 4.95, "Wrong value in row 0 column 2")
                                
                                var index = Index("\"index\"", on: t, columns: [tNew.a, desc(t.b)])
                                index.create(connection: connection) { result in
                                    XCTAssertEqual(result.success, false, "CREATE INDEX should fail")
                                    XCTAssertNotNil(result.asError, "CREATE INDEX should return an error")
                                    XCTAssertEqual("\(result.asError!)", "Index contains columns that do not belong to its table.")
                                    
                                    index = Index("\"index\"", on: t, columns: [t.a, desc(t.b)])
                                    index.create(connection: connection) { result in
                                        XCTAssertEqual(result.success, true, "CREATE INDEX failed")
                                        XCTAssertNil(result.asError, "Error in CREATE INDEX: \(result.asError!)")
                                        
                                        index.drop(connection: connection) { result in
                                            XCTAssertEqual(result.success, true, "DROP INDEX failed")
                                            XCTAssertNil(result.asError, "Error in DROP INDEX: \(result.asError!)")
                                            
                                            let migration = Migration(from: t, to: tNew, using: connection)
                                            migration.alterTableName() { result in
                                                XCTAssertEqual(result.success, true, "Migration failed")
                                                XCTAssertNil(result.asError, "Error in Migration: \(result.asError!)")
                                                
                                                migration.alterTableAdd(column: tNew.d) { result in
                                                    XCTAssertEqual(result.success, true, "Migration failed")
                                                    XCTAssertNil(result.asError, "Error in Migration: \(result.asError!)")
                                                    
                                                    let s2 = Select(from: tNew)
                                                    executeQuery(query: s2, connection: connection) { result, rows in
                                                        XCTAssertEqual(result.success, true, "SELECT failed")
                                                        XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                                        XCTAssertNotNil(rows, "SELECT returned no rows")
                                                        
                                                        let resultSet = result.asResultSet!
                                                        XCTAssertEqual(resultSet.titles.count, 4, "SELECT returned wrong number of titles")
                                                        XCTAssertEqual(resultSet.titles[0], "a", "Wrong column name for column 0")
                                                        XCTAssertEqual(resultSet.titles[1], "b", "Wrong column name for column 1")
                                                        XCTAssertEqual(resultSet.titles[2], "c", "Wrong column name for column 2")
                                                        XCTAssertEqual(resultSet.titles[3], "d", "Wrong column name for column 3")
                                                        
                                                        XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                                                        XCTAssertEqual(rows![0].count, 4, "SELECT returned wrong number of columns")
                                                        XCTAssertEqual(rows![0][0]! as? String, "apple", "Wrong value in row 0 column 0")
                                                        XCTAssertEqual(rows![0][1]! as? sqliteInt, 5, "Wrong value in row 0 column 1")
                                                        XCTAssertEqual(rows![0][2]! as? Double, 4.95, "Wrong value in row 0 column 2")
                                                        XCTAssertEqual(rows![0][3]! as? sqliteInt, 123, "Wrong value in row 0 column 3")
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
            expectation.fulfill()
        })
    }
    
    class Table1: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self, primaryKey: true)
        let c = Column("c", Double.self, defaultValue: 4.95)
        
        let tableName = "Table1" + tableNameSuffix
    }
    
    class Table2: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)
        
        let tableName = "Table2" + tableNameSuffix
    }
    
    class Table3: Table {
        let a = Column("a", String.self, defaultValue: "qiwi")
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self, defaultValue: 4.95)
        let d = Column("d", Int32.self, defaultValue: 123)
        
        let tableName = "Table3" + tableNameSuffix
    }
    
    
    func testPrimaryKeys() {
        let t1 = Table1()
        let t2 = Table2()
        let t3 = Table3()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t1.tableName, connection: connection) { result in
                cleanUp(table: t2.tableName, connection: connection) { result in
                    cleanUp(table: t3.tableName, connection: connection) { result in
                        
                        t1.create(connection: connection) { result in
                            XCTAssertEqual(result.success, false, "CREATE TABLE with conflicting primary keys didn't fail")
                            XCTAssertEqual("\(result.asError!)", "Conflicting definitions of primary key. ", "Wrong error")
                            
                            t2.primaryKey(t2.c, t2.d).create(connection: connection) { result in
                                XCTAssertEqual(result.success, false, "CREATE TABLE with conflicting primary keys didn't fail")
                                XCTAssertEqual("\(result.asError!)", "Conflicting definitions of primary key. ", "Wrong error")
                                
                                t3.primaryKey(t3.c, t3.d).create(connection: connection) { result in
                                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                                }
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    
    class Table4: Table {
        let a = Column("a", String.self)
        let b = Column("b", Int32.self)
        let c = Column("c", Double.self)
        
        let tableName = "Table4" + tableNameSuffix
    }
    
    class Table5: Table {
        let e = Column("e", String.self, primaryKey: true)
        let f = Column("f", Int32.self)
        
        let tableName = "Table5" + tableNameSuffix
    }
    
    func testForeignKeys() {
        let t4 = Table4()
        let t5 = Table5()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t5.tableName, connection: connection) { result in
                cleanUp(table: t4.tableName, connection: connection) { result in
                    
                    t4.primaryKey(t4.a, t4.b).create(connection: connection) { result in
                        XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                        XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                        
                        t5.foreignKey([t5.e, t5.f], references: [t4.a, t4.b]).create(connection: connection) { result in
                            XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                            XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                            
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
    
    
    
    class TypesTable: Table {
        let a = Column("a", Varchar.self, length: 30, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Varchar.self, length: 10)
        let c = Column("c", Char.self, length: 10)
        
        let d = Column("d", Int16.self)
        let e = Column("e", Int32.self)
        let f = Column("f", Int64.self)
        
        let g = Column("g", Float.self)
        let h = Column("h", Double.self)
        
        let i = Column("i", SQLDate.self)
        let j = Column("j", Time.self)
        let k = Column("k", Timestamp.self)
        
        let tableName = "TypesTable" + tableNameSuffix
    }
    
    
    func testTypes() {
        let t = TypesTable()
        
        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in
            
            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            
            cleanUp(table: t.tableName, connection: connection) { result in
                
                t.create(connection: connection) { result in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError!)")
                    
                    let now = Date()
                    
                    let i1 = Insert(into: t, values: "apple", "passion fruit", "peach", 123456789, 123456789, 123456789, -0.53, 123.4567, now, now, now)
                    executeQuery(query: i1, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")
                        
                        let s1 = Select(from: t)
                        executeQuery(query: s1, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "SELECT failed")
                            XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                            XCTAssertNotNil(rows, "SELECT returned no rows")
                            
                            XCTAssertEqual(rows!.count, 1, "SELECT returned wrong number of rows")
                            XCTAssertEqual(rows![0].count, 11, "SELECT returned wrong number of columns")
                            XCTAssertEqual(rows![0][0]! as? String, "apple", "Wrong value in row 0 column 0")
                            XCTAssertEqual(rows![0][1]! as? String, "passion fruit", "Wrong value in row 0 column 1")
                            XCTAssertEqual(rows![0][2]! as? String, "peach", "Wrong value in row 0 column 2")
                            XCTAssertEqual(rows![0][3]! as? sqliteInt, 123456789, "Wrong value in row 0 column 3")
                            XCTAssertEqual(rows![0][4]! as? sqliteInt, 123456789, "Wrong value in row 0 column 4")
                            XCTAssertEqual(rows![0][5]! as? sqliteInt, 123456789, "Wrong value in row 0 column 5")
                            XCTAssertEqual(rows![0][6]! as? Double, -0.53, "Wrong value in row 0 column 6")
                            XCTAssertEqual(rows![0][7]! as? Double, 123.4567, "Wrong value in row 0 column 7")
                            XCTAssertEqual(rows![0][8]! as? String, "\(now)", "Wrong value in row 0 column 8")
                            XCTAssertEqual(rows![0][9]! as? String, "\(now)", "Wrong value in row 0 column 9")
                            XCTAssertEqual(rows![0][10]! as? String, "\(now)", "Wrong value in row 0 column 10")
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }

    class AutoIncrement1: Table {
        let a = Column("a", String.self, defaultValue: "qiwi")
        let b = Column("b", Int32.self, autoIncrement: true, primaryKey: true)

        let tableName = "AutoIncrement1" + tableNameSuffix
    }

    class AutoIncrement2: Table {
        let a = Column("a", String.self, primaryKey: true, defaultValue: "qiwi")
        let b = Column("b", Int32.self, autoIncrement: true)

        let tableName = "AutoIncrement2" + tableNameSuffix
    }

    class AutoIncrement3: Table {
        let a = Column("a", String.self, defaultValue: "qiwi")
        let b = Column("b", String.self, autoIncrement: true, primaryKey: true)

        let tableName = "AutoIncrement3" + tableNameSuffix
    }

    func testAutoIncrement() {
        let t1 = AutoIncrement1()
        let t2 = AutoIncrement2()
        let t3 = AutoIncrement3()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }
            cleanUp(table: t1.tableName, connection: connection) { result in
                cleanUp(table: t2.tableName, connection: connection) { result in
                    cleanUp(table: t3.tableName, connection: connection) { result in

                        t1.create(connection: connection) { result in
                            XCTAssertEqual(result.success, true, "CREATE TABLE failed for \(t1.tableName)")

                            t2.create(connection: connection) { result in
                                XCTAssertEqual(result.success, false, "CREATE TABLE non primary key auto increment column didn't fail")

                                t3.create(connection: connection) { result in
                                    XCTAssertEqual(result.success, false, "CREATE TABLE non integer auto increment column didn't fail")
                                }
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }

    class int64Table: Table {
        let int64 = Column("int64", Int64.self)

        let tableName = "int64Table" + tableNameSuffix
    }

    func testInt64() {

        let i64 = int64Table()

        let pool = CommonUtils.sharedInstance.getConnectionPool()
        performTest(asyncTasks: { expectation in

            guard let connection = pool.getConnection() else {
                XCTFail("Failed to get connection")
                return
            }

            cleanUp(table: i64.tableName, connection: connection) { result in
                i64.create(connection: connection) { result in
                    XCTAssertEqual(result.success, true, "CREATE TABLE failed for \(i64.tableName)")
                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                    let insert = Insert(into: i64, values: INT32_MAX)
                    executeQuery(query: insert, connection: connection) { result, rows in
                        XCTAssertEqual(result.success, true, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                        let overflow: Int64 = Int64(INT32_MAX)
                        let insert2 = Insert(into: i64, values: overflow + 1)
                        executeQuery(query: insert2, connection: connection) { result, rows in
                            XCTAssertEqual(result.success, true, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError!)")

                            let select = Select(from: i64)
                            executeQuery(query: select, connection: connection) { result, rows in
                                XCTAssertEqual(result.success, true, "SELECT failed")
                                XCTAssertNotNil(result.asResultSet, "SELECT returned no rows")
                                XCTAssertNotNil(rows, "SELECT returned no rows")

                                XCTAssertEqual(rows!.count, 2, "SELECT returned wrong number of rows")
                                XCTAssertEqual(rows![0].count, 1, "SELECT returned wrong number of columns")
                                XCTAssertEqual(rows![0][0]! as? sqliteInt, 2147483647, "Wrong value in row 0 column 0")
                                XCTAssertEqual(rows![1][0]! as? sqliteInt, 2147483648, "Wrong value in row 1 column 0")
                                XCTAssertNil(rows![0][0]! as? Int32)
                            }
                        }
                    }
                }
            }
            expectation.fulfill()
        })
    }
}
