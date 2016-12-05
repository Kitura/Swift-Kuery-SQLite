import XCTest
@testable import SwiftKuerySQLiteTests

XCTMain([
     testCase(TestSelect.allTests),
     testCase(TestInsert.allTests),
     testCase(TestUpdate.allTests),
     testCase(TestAlias.allTests),
     testCase(TestJoin.allTests),
     testCase(TestSubquery.allTests),
])
