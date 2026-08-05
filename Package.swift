// swift-tools-version: 5.9
import PackageDescription

let package = Package(
    name: "BsSolutionsBastionNetworkPlugin",
    platforms: [.iOS(.v15)],
    products: [
        .library(
            name: "BsSolutionsBastionNetworkPlugin",
            targets: ["BastionNetworkPlugin"])
    ],
    dependencies: [
        .package(url: "https://github.com/ionic-team/capacitor-swift-pm.git", from: "6.2.0")
    ],
    targets: [
        .target(
            name: "BastionNetworkPlugin",
            dependencies: [
                .product(name: "Capacitor", package: "capacitor-swift-pm"),
                .product(name: "Cordova", package: "capacitor-swift-pm")
            ],
            path: "ios/Sources/BastionNetworkPlugin"),
        .testTarget(
            name: "BastionNetworkPluginTests",
            dependencies: ["BastionNetworkPlugin"],
            path: "ios/Tests/BastionNetworkPluginTests")
    ]
)
