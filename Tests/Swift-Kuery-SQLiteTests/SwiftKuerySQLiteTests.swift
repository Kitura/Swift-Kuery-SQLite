import XCTest
import SwiftKuery
@testable import SwiftKuerySQLite

class SwiftKuerySQLiteTests: XCTestCase {
    
    public struct MyTable : Table {
        let a = Column("a")
        let b = Column("b")
        
        public var name = "myTable"
    }
    
    
    func testExample() {
        //setup db
        let connection = SwiftKuerySQLite(filename: "/Users/robertdeans/Documents/Swift-Kuery-SQLite/Tests/Swift-Kuery-SQLiteTests/testDb.db", options: [ConnectionOptions.readOnly(false)])

        connection.connect() { response in
            XCTAssertNotNil(response, "Error opening to SQLite server: \(response)")
            
            let t = MyTable()
            
            print("=======CREATE TABLE mytable (a varchar(40), b integer)=======")
            connection.execute("CREATE TABLE mytable ("
                + "a varchar(40), "
                + "b integer)") { result in
                    XCTAssertTrue(result.success, "CREATE TABLE failed")
                    XCTAssertNil(result.asError, "Error in CREATE TABLE: \(result.asError)")
                    
                    let i1 = Insert(into: t, values: "apple", 10)
                    print("=======\(connection.descriptionOf(query: i1))=======")
                    connection.execute(query: i1) { result in
                        XCTAssertTrue(result.success, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                    }
                    
                    let i2 = Insert(into: t, valueTuples: (t.a, "appricot"), (t.b, "3"))
                    print("=======\(connection.descriptionOf(query: i2))=======")
                    connection.execute(query: i2) { result in
                        XCTAssertTrue(result.success, "INSERT failed")
                        XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                        
                        let i3 = Insert(into: t, columns: [t.a, t.b], values: ["banana", 17])
                        print("=======\(connection.descriptionOf(query: i3))=======")
                        connection.execute(query: i3) { result in
                            XCTAssertTrue(result.success, "INSERT failed")
                            XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                            
                            
                            
                            let i4 = Insert(into: t, rows: [["apple", 17], ["banana", -7], ["banana", 27]])
                            print("=======\(connection.descriptionOf(query: i4))=======")
                            connection.execute(query: i4) { result in
                                XCTAssertTrue(result.success, "INSERT failed")
                                XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                
                                
                                
                                let s1 = Select(from: t)
                                print("=======\(connection.descriptionOf(query: s1))=======")
                                connection.execute(query: s1) { result in
                                    XCTAssertTrue(result.success, "SELECT failed")
                                    XCTAssertNil(result.asError, "Error in INSERT: \(result.asError)")
                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                    let (_, rows) = result.asRows!
                                    XCTAssertEqual(rows.count, 6, "SELECT returned wrong number of rows: \(rows.count) instead of 6")
                                    
                                    
                                    
                                    let sd1 = Select.distinct(t.a, from: t)
                                        .where(t.a.like("b%"))
                                    print("=======\(connection.descriptionOf(query: sd1))=======")
                                    connection.execute(query: sd1) { result in
                                        XCTAssertTrue(result.success, "SELECT failed")
                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                        let (_, rows) = result.asRows!
                                        XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                        
                                        
                                        
                                        let s3 = Select(t.b, t.a, from: t)
                                            .where(((t.a == "banana") || (ucase(t.a) == "APPLE")) && (t.b == 27 || t.b == -7 || t.b == 17))
                                            .order(by: .ASCD(t.b), .DESC(t.a))
                                        print("=======\(connection.descriptionOf(query: s3))=======")
                                        connection.execute(query: s3) { result in
                                            XCTAssertTrue(result.success, "SELECT failed")
                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                            let (titles, rows) = result.asRows!
                                            XCTAssertEqual(rows.count, 4, "SELECT returned wrong number of rows: \(rows.count) instead of 4")
                                            XCTAssertEqual(titles[0], "b", "Wrong column name: \(titles[0]) instead of b")
                                            XCTAssertEqual(titles[1], "a", "Wrong column name: \(titles[1]) instead of a")
                                            
                                            
                                            
                                            let s4 = Select(t.a, from: t)
                                                .where(t.b >= 0)
                                                .group(by: t.a)
                                                .order(by: .DESC(t.a))
                                                .having(sum(t.b) > 3)
                                            print("=======\(connection.descriptionOf(query: s4))=======")
                                            connection.execute(query: s4) { result in
                                                XCTAssertTrue(result.success, "SELECT failed")
                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                let (titles, rows) = result.asRows!
                                                XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                XCTAssertEqual(titles[0], "a", "Wrong column name: \(titles[0]) instead of a")
                                                XCTAssertEqual(rows[0][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[0][0]) instead of banana")
                                                XCTAssertEqual(rows[1][0]! as! String, "apple", "Wrong value in row 1 column 0: \(rows[1][0]) instead of apple")
                                                
                                                
                                                
                                                /*let s4Raw = Select(RawField("left(a, 2) as raw"), from: t)
                                                    .where("b >= 0")
                                                    .group(by: t.a)
                                                    .order(by: .DESC(t.a))
                                                    .having("sum(b) > 3")
                                                print("=======\(connection.descriptionOf(query: s4Raw))=======")
                                                connection.execute(query: s4Raw) { result in
                                                    XCTAssertTrue(result.success, "SELECT failed")
                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                    let (titles, rows) = result.asRows!
                                                    XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                    XCTAssertEqual(titles[0], "raw", "Wrong column name: \(titles[0]) instead of raw")
                                                    XCTAssertEqual(rows[0][0]! as! String, "ba", "Wrong value in row 0 column 0: \(rows[0][0]) instead of ba")
                                                    XCTAssertEqual(rows[1][0]! as! String, "ap", "Wrong value in row 1 column 0: \(rows[1][0]) instead of ap")*/
                                                    
                                                    
                                                    
                                                    let s5 = Select(t.a, t.b, from: t)
                                                        .limit(to: 2)
                                                        .order(by: .DESC(t.a))
                                                    print("=======\(connection.descriptionOf(query: s5))=======")
                                                    connection.execute(query: s5) { result in
                                                        XCTAssertTrue(result.success, "SELECT failed")
                                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                        let (_, rows) = result.asRows!
                                                        XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                        XCTAssertEqual(rows[0][0]! as! String, "banana", "Wrong value in row 0 column 0: \(rows[0][0]) instead of banana")
                                                        XCTAssertEqual(rows[1][0]! as! String, "banana", "Wrong value in row 1 column 0: \(rows[1][0]) instead of banana")
                                                        
                                                        
                                                        let u1 = Update(table: t, set: [(t.a, "peach"), (t.b, 2)])
                                                            .where(t.a == "banana")
                                                        print("=======\(connection.descriptionOf(query: u1))=======")
                                                        connection.execute(query: u1) { result in
                                                            XCTAssertTrue(result.success, "UPDATE failed")
                                                            XCTAssertNil(result.asError, "Error in UPDATE: \(result.asError)")
                                                            
                                                            
                                                            
                                                            let s6 = Select(t.a, t.b, from: t)
                                                                .where(t.a == "banana")
                                                            print("=======\(connection.descriptionOf(query: s6))=======")
                                                            connection.execute(query: s6) { result in
                                                                XCTAssertTrue(result.success, "SELECT failed")
                                                                XCTAssertNil(result.asRows, "SELECT returned  rows")

                                                                
                                                                let d2 = Delete(from: t)
                                                                    .where(t.b == "2")
                                                                print("=======\(connection.descriptionOf(query: d2))=======")
                                                                connection.execute(query: d2) { result in
                                                                    XCTAssertTrue(result.success, "DELETE failed")
                                                                    XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                                    
                                                                    
                                                                    
                                                                    let s7 = Select(ucase(t.a).as("upper"), t.b, from: t)
                                                                        .where(t.a.between("appra", and: "apprt"))
                                                                    print("=======\(connection.descriptionOf(query: s7))=======")
                                                                    connection.execute(query: s7) { result in
                                                                        XCTAssertTrue(result.success, "SELECT failed")
                                                                        XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                        let (titles, rows) = result.asRows!
                                                                        XCTAssertEqual(rows.count, 1, "SELECT returned wrong number of rows: \(rows.count) instead of 1")
                                                                        XCTAssertEqual(titles[0], "upper", "Wrong column name: \(titles[0]) instead of upper")
                                                                        XCTAssertEqual(rows[0][0]! as! String, "APPRICOT", "Wrong value in row 0 column 0: \(rows[0][0]) instead of APPRICOT")
                                                                        
                                                                        
                                                                        
                                                                        let s8 = Select(from: t)
                                                                            .where(t.a.in("apple", "lalala"))
                                                                        print("=======\(connection.descriptionOf(query: s8))=======")
                                                                        connection.execute(query: s8) { result in
                                                                            XCTAssertTrue(result.success, "SELECT failed")
                                                                            XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                            let (_, rows) = result.asRows!
                                                                            XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                                            XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                                            
                                                                            
                                                                            
                                                                            let s9 = Select(from: t)
                                                                                .where("a IN ('apple', 'lalala')")
                                                                            print("=======\(connection.descriptionOf(query: s9))=======")
                                                                            connection.execute(query: s9) { result in
                                                                                XCTAssertTrue(result.success, "SELECT failed")
                                                                                XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                                let (_, rows) = result.asRows!
                                                                                XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                                                XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                                                
                                                                                
                                                                                
                                                                                let s10 = "Select * from myTable where a IN ('apple', 'lalala')"
                                                                                print("=======\(s10)=======")
                                                                                connection.execute(s10) { result in
                                                                                    XCTAssertTrue(result.success, "SELECT failed")
                                                                                    XCTAssertNotNil(result.asRows, "SELECT returned no rows")
                                                                                    let (_, rows) = result.asRows!
                                                                                    XCTAssertEqual(rows.count, 2, "SELECT returned wrong number of rows: \(rows.count) instead of 2")
                                                                                    XCTAssertEqual(rows[0][0]! as! String, "apple", "Wrong value in row 0 column 0: \(rows[0][0]) instead of apple")
                                                                                    
                                                                                    
                                                                                    let d1 = Delete(from: t)
                                                                                    print("=======\(connection.descriptionOf(query: d1))=======")
                                                                                    connection.execute(query: d1) { result in
                                                                                        XCTAssertTrue(result.success, "DELETE failed")
                                                                                        XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                                                        
                                                                                        let s1 = Select(from: t)
                                                                                        print("=======\(connection.descriptionOf(query: s1))=======")
                                                                                        s1.execute(connection) { result in
                                                                                            XCTAssertTrue(result.success, "SELECT failed")
                                                                                            XCTAssertNil(result.asRows, "SELECT returned some rows")

                                                                                            let drop = Raw(query: "DROP TABLE", table: t)
                                                                                            print("=======\(connection.descriptionOf(query: drop))=======")
                                                                                            drop.execute(connection) { result in
                                                                                                XCTAssertTrue(result.success, "DROP TABLE failed")
                                                                                                XCTAssertNil(result.asError, "Error in DELETE: \(result.asError)")
                                                                                            }
                                                                                        }
                                                                                    }
                                                                                }
                                                                            }
                                                                        }
                                                                    }
        //connection.closeConnection()
                                                                }
                                                            }
                                                        }
                                                    //}
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


    static var allTests : [(String, (SwiftKuerySQLiteTests) -> () throws -> Void)] {
        return [
            ("testExample", testExample),
        ]
    }
}
