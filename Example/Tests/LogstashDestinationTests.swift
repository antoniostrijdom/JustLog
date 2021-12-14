//
//  LogstashDestinationTests.swift
//  JustLog_Tests
//
//  Created by Antonio Strijdom on 13/07/2020.
//  Copyright © 2020 Just Eat. All rights reserved.
//

import XCTest
@testable import JustLog

class LogstashDestinationTests: XCTestCase {

    func testBasicLogging() throws {
        let expect = expectation(description: "Send log expectation")
        let mockSocket = MockLogstashDestinationSocket(host: "", port: 0, timeout: 5, logActivity: true, allowUntrustedServer: true)
        mockSocket.networkOperationCountExpectation = expect
        mockSocket.completionHandlerCalledExpectation = expectation(description: "completionHandlerCalledExpectation")
        let destination = LogstashDestination(socket: mockSocket, logActivity: true)
        _ = destination.send(.verbose, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.debug, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.info, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.warning, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.error, msg: "{}", thread: "", file: "", function: "", line: 0)
        expect.expectedFulfillmentCount = 5
        try destination.forceSend()
        self.waitForExpectations(timeout: 10.0, handler: nil)
    }

    func testLoggingError() throws {
        let expect = expectation(description: "Error log expectation")
        let expectation1 = expectation(description: "First completion expectation")
        let mockSocket = MockLogstashDestinationSocket(host: "", port: 0, timeout: 5, logActivity: true, allowUntrustedServer: true)
        mockSocket.errorState = true
        mockSocket.networkOperationCountExpectation = expect
        mockSocket.completionHandlerCalledExpectation = expectation1
        let destination = LogstashDestination(socket: mockSocket, logActivity: true)
        _ = destination.send(.verbose, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.debug, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.info, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.warning, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.error, msg: "{}", thread: "", file: "", function: "", line: 0)
        expect.expectedFulfillmentCount = 5
        try destination.forceSend()
        self.waitForExpectations(timeout: 10.0, handler: nil)
        mockSocket.errorState = false
        let expect2 = expectation(description: "Send log expectation")
        expect2.expectedFulfillmentCount = 5
        let expectation2 = expectation(description: "Second completion expectation")
        mockSocket.networkOperationCountExpectation = expect2
        mockSocket.completionHandlerCalledExpectation = expectation2
        try destination.forceSend()
        self.waitForExpectations(timeout: 10.0, handler: nil)
    }
    
    func testLoggingCancel() throws {
        let expect = expectation(description: "Error log expectation")
        let expectation1 = expectation(description: "First completion expectation")
        let mockSocket = MockLogstashDestinationSocket(host: "", port: 0, timeout: 5, logActivity: true, allowUntrustedServer: true)
        mockSocket.errorState = true
        mockSocket.networkOperationCountExpectation = expect
        mockSocket.completionHandlerCalledExpectation = expectation1
        let destination = LogstashDestination(socket: mockSocket, logActivity: true)
        _ = destination.send(.verbose, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.debug, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.info, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.warning, msg: "{}", thread: "", file: "", function: "", line: 0)
        _ = destination.send(.error, msg: "{}", thread: "", file: "", function: "", line: 0)
        expect.expectedFulfillmentCount = 5
        try destination.forceSend()
        self.waitForExpectations(timeout: 10.0, handler: nil)
        destination.cancelSending()
        mockSocket.errorState = false
        let expect2 = expectation(description: "Send log expectation")
        let expectation2 = expectation(description: "Second completion expectation")
        mockSocket.networkOperationCountExpectation = expect2
        mockSocket.completionHandlerCalledExpectation = expectation2
        _ = destination.send(.error, msg: "{}", thread: "", file: "", function: "", line: 0)
        try destination.forceSend()
        self.waitForExpectations(timeout: 10.0, handler: nil)
    }
}

enum LogstashDestinationTestError: Error {
    case whoops
}

class MockLogstashDestinationSocket: NSObject, LogstashDestinationSocketProtocol {
    
    var networkOperationCountExpectation: XCTestExpectation?
    var completionHandlerCalledExpectation: XCTestExpectation?
    var errorState: Bool = false
    
    required init(host: String, port: UInt16, timeout: TimeInterval, logActivity: Bool, allowUntrustedServer: Bool) {
        super.init()
    }
    
    func cancel() {
        // do nothing
    }
    
    func sendLogs(_ logs: [LogTag: LogContent],
                  transform: (LogContent) -> Data) async -> LogstashDestinationSocketProtocolSendResult {
        
        var sendStatus = [Int: Error]()
        for log in logs.sorted(by: { $0.0 < $1.0 }) {
            let tag = log.0
            _ = transform(log.1)
            try! await Task.sleep(nanoseconds: 1000)
            if let error: LogstashDestinationTestError? = self.errorState ? .whoops : nil {
                sendStatus[tag] = error
            }
            print("sendLogs - sent")
            self.networkOperationCountExpectation?.fulfill()
        }
        
        print("sendLogs - \(logs.count) sent")
        self.completionHandlerCalledExpectation?.fulfill()
        return sendStatus
    }
}
