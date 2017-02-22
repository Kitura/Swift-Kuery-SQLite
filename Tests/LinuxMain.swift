import XCTest
@testable import SwiftKuerySQLiteTests

XCTMain([
     testCase(TestAlias.allTests),
     testCase(TestInsert.allTests),
     testCase(TestJoin.allTests),
     testCase(TestParameters.allTests),
     testCase(TestSelect.allTests),
     testCase(TestSubquery.allTests),
     testCase(TestUpdate.allTests),
     testCase(TestWith.allTests),
     testCase(TestTransaction.allTests),
])
