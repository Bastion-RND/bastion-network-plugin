import XCTest
@testable import BastionNetworkPlugin

final class BastionNetworkPluginTests: XCTestCase {
    func testPluginLoads() {
        XCTAssertNotNil(BastionNetworkPlugin())
    }
}
