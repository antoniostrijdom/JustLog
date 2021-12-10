//
//  LogstashDestination.swift
//  JustLog
//
//  Created by Shabeer Hussain on 06/12/2016.
//  Copyright © 2017 Just Eat. All rights reserved.
//

import Foundation
import SwiftyBeaver

typealias LogContent = [String: Any]
typealias LogTag = Int

private actor LogQueueManager {
    private let shouldLogActivity: Bool
    private let socket: LogstashDestinationSocketProtocol
    private var logsToShip = [LogTag: LogContent]()
    
    init(socket: LogstashDestinationSocketProtocol,
         shouldLogActivity: Bool) {
        self.socket = socket
        self.shouldLogActivity = shouldLogActivity
    }
    
    deinit {
        self.cancelSending()
    }
    
    private func printActivity(_ string: String) {
        guard shouldLogActivity else { return }
        print(string)
    }
    
    func addLog(_ dict: LogContent) {
        let time = mach_absolute_time()
        let logTag = Int(truncatingIfNeeded: time)
        self.logsToShip[logTag] = dict
    }
    
    func send() async throws {
        let writer = LogstashDestinationWriter(socket: self.socket, shouldLogActivity: shouldLogActivity)
        let logsBatch = logsToShip
        logsToShip = [LogTag: LogContent]()
        let result = await writer.write(logs: logsBatch)
        if let unsent = result.0 {
            logsToShip.merge(unsent) { lhs, rhs in lhs }
            self.printActivity("🔌 <LogstashDestination>, \(unsent.count) failed tasks")
        }
        if let error = result.1 {
            throw error
        }
    }
    
    func cancelSending() {
        self.logsToShip = [LogTag: LogContent]()
        self.socket.cancel()
    }
}

public class LogstashDestination: BaseDestination  {
    
    /// Settings
    var shouldLogActivity: Bool
    public var logzioToken: String?
    
    /// Logs buffer
    private let logQueue: LogQueueManager
    /// Private
    private let logzioTokenKey = "token"
    
    @available(*, unavailable)
    override init() {
        fatalError()
    }
    
    public required init(socket: LogstashDestinationSocketProtocol, logActivity: Bool) {
        self.logQueue = LogQueueManager(socket: socket, shouldLogActivity: logActivity)
        self.shouldLogActivity = logActivity
        super.init()
    }
    
    func cancelSending() {
        Task {
            await logQueue.cancelSending()
        }
    }
    
    // MARK: - Log dispatching

    override public func send(_ level: SwiftyBeaver.Level,
                              msg: String,
                              thread: String,
                              file: String,
                              function: String,
                              line: Int,
                              context: Any? = nil) -> String? {
        Task {
            if let dict = msg.toDictionary() {
                var flattened = dict.flattened()
                if let logzioToken = logzioToken {
                    flattened = flattened.merged(with: [logzioTokenKey: logzioToken])
                }
                await logQueue.addLog(flattened)
            }
        }
        
        return nil
    }

    public func forceSend() throws {
        Task {
            try await logQueue.send()
        }
    }
}

extension LogstashDestination {
    
    private func printActivity(_ string: String) {
        guard shouldLogActivity else { return }
        print(string)
    }
}
