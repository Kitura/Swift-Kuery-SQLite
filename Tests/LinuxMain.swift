import XCTest
@testable import SwiftKuerySQLiteTests

XCTMain([
     testCase(TestSelect.allTests),
     testCase(TestInsert.allTests),
     testCase(TestUpdate.allTests),
     testCase(TestAlias.allTests),
     testCase(TestJoin.allTests),
     testCase(TestParameters.allTests),
     testCase(TestSubquery.allTests),
     testCase(TestWith.allTests),
])
