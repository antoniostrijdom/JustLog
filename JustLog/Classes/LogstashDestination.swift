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
    private(set) var logsToShip = [LogTag: LogContent]()
    
    func addLog(_ dict: LogContent) {
        let time = mach_absolute_time()
        let logTag = Int(truncatingIfNeeded: time)
        logsToShip[logTag] = dict
    }
    
    func reset() {
        logsToShip.removeAll()
    }
    
    func mergeLogs(_ logsToMerge : [LogTag: LogContent]) {
        logsToShip.merge(logsToMerge) { lhs, rhs in lhs }
    }
}

public class LogstashDestination: BaseDestination  {
    
    /// Settings
    var shouldLogActivity: Bool
    public var logzioToken: String?
    
    /// Logs buffer
    private let logQueue = LogQueueManager()
    /// Socket
    private let socket: LogstashDestinationSocketProtocol
    /// Private
    private let logzioTokenKey = "token"
    
    @available(*, unavailable)
    override init() {
        fatalError()
    }
    
    public required init(socket: LogstashDestinationSocketProtocol, logActivity: Bool) {
        self.socket = socket
        self.shouldLogActivity = logActivity
        super.init()
    }
    
    deinit {
        cancelSending()
    }
    
    func send() async throws {
        let writer = LogstashDestinationWriter(socket: self.socket, shouldLogActivity: shouldLogActivity)
        let logsBatch = await logQueue.logsToShip
        await logQueue.reset()
        let result = await writer.write(logs: logsBatch)
        if let unsent = result.0 {
            await logQueue.mergeLogs(unsent)
            self.printActivity("🔌 <LogstashDestination>, \(unsent.count) failed tasks")
        }
        if let error = result.1 {
            throw error
        }
    }
    
    func cancelSending() {
        Task {
            await logQueue.reset()
        }
        self.socket.cancel()
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
            try await send()
        }
    }
}

extension LogstashDestination {
    
    private func printActivity(_ string: String) {
        guard shouldLogActivity else { return }
        print(string)
    }
}
