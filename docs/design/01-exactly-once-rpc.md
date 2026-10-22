# Design: Exactly-Once RPC over an Unreliable Network

*Author: Hinyeung Lam*

## Preface

- **Goals:** The system aims to provide an "exactly-once" RPC for a client-server key-value store architecture, ensuring that operations are executed exactly once despite network failures.
- **Desired fault model:** The system handles message loss, duplication, and reordering in an asynchronous network. It should work even when messages between clients and the server are arbitrarily delayed, dropped, duplicated, or reordered.
- **Challenges:**
    - Ensuring exactly once semantics in an unreliable network
    - Handling retries without executing commands multiple times
    - Handling out-of-order message delivery
    - Garbage collecting old command/result records to prevent memory leaks
    - Preventing old operations from overwriting newer state
- **Assumptions:**
    - Clients have at most one outstanding request at a time
    - Nodes (clients and server) don't crash permanently
    - The server runs a deterministic application
    - Each client has a unique address

## Protocol

- **Kinds of node:**
    - Client: always acts as a requester of operations
    - Server: always acts as a processor of operations
- **State at each kind of node:**
    - **Client State:**
        - serverAddress: Address - The address of the server (init at client creation)
        - sequenceNum: int - The current sequence number for commands (init: 0)
        - currentCommand: Command - The command currently being processed (init: null)
        - amoCommand: AMOCommand - The wrapped command currently being processed (init: null)
        - result: Result - The result for the current command, if available (init: null)
    - **Server State:**
        - amoApplication: AMOApplication - A wrapper around the application that ensures at-most-once execution
        - application: Application - The underlying application
        - results: Map\<Address, Map\<Integer, Result\>\> - Results of previously executed commands (init: empty map)
        - highestSequenceNums: Map\<Address, Integer\> - Highest sequence number seen from each client (init: empty map)
- **Messages:**
    - **Request:**
        - **Source:** Client
        - **Destination:** Server
        - **Contents:** AMOCommand
        - **When is it sent:** When a client wants to execute a command or retry after a timeout
            - Can be sent spontaneously, when client's sendCommand() is called
        - What happens at the destination when it is received?
            - If the server has already executed this command (same client address and sequence number), it returns the cached result without re-executing.
            - If this is an old command (sequence number \< highest seen) that modifies state, return a fabricated result without executing.
            - Otherwise, execute the command on the application, store the result, and send a reply.
            - Server also performs garbage collection of old results.
    - **Request:**
        - **Source:** Server
        - **Destination:** Client
        - **Contents:** AMOResult
        - **When is it sent:** After the server executes a command or gets a cached hit
            - Can be only sent in response to another message/timer
        - What happens at the destination when it is received?
            - If the sequence number matches the client's current sequence number, store the result and notify waiting threads.
            - If the sequence number doesn't match, ignore the reply
            - If the result is stored, increment the sequence number for the next request.
- **Timers:**
    - **ClientTimer:**
        - What kind of node sets it? Client
        - **Contents:** AMOCommand (the command being retried)
        - When is it set? When a client sends a command, waiting for a response
        - What happens when it fires?
            - If the timer's command matches the current command and no result has been received, resend the Request message and reset the timer.

## Correctness/Liveness Analysis

- For each message, why is the protocol correct/live even if the network delays, reorders, drops, or duplicates that message?
    - Request: Clients use timers to retry timeout requests, and servers use sequence numbers to detect and handle duplicate or out-of-order commands.
    - Reply: Clients will continue to retry their requests if replies are lost or delayed, and they verify sequence numbers to handle out-of-order replies correctly.
- Why is the protocol correct/live even if the network delays or drops a group of messages?
    - The client will continue to retry sending its request with the same sequence number until it receives a response, regardless of how many messages are lost or delayed. The server's AMOApplication stores results for received commands to handle retries correctly.
- For each node, why is the protocol correct/live even if that node crashes or disappears (doesn't send or receive messages) for a long time and then later is reconnected?
    - Clients: When a client reconnects, it will continue with its last sequence number and retry the last failed request.
    - Server: When a server reconnects, it will handle new requests correctly despite previous state is lost, since the server doesn't (and doesn’t have to) persist state across crashes.
- Why is the protocol correct/live even when a group of nodes are partitioned from some but not all nodes, and then the partition is resolved.
    - Since our system has only a single server, a partition would simply disconnect some clients from the server. When the partition resolves, clients will continue retrying their requests with the same sequence numbers, and the server's AMOApplication will handle these correctly, maintaining exactly-once semantics.

## Conclusion

- **Goals achieved:**
    - Exactly-once execution: The protocol achieves this by adding a shim layer (server-side deduplication using sequence numbers) that ensures each unique command is executed at most once to the current at-least-once RPC.
    - Handling network failures: The system handles message loss through retries, duplication through deduplication at the server, and reordering using sequence number.
    - State consistency: The system prevents old commands from overwriting newer state by tracking the highest sequence number from each client.
    - Memory efficiency: Garbage collection of old results prevents unbounded memory growth while maintaining enough history for correct handling of retries.
- **Limitations:**
    - The system assumes clients with at most one outstanding request at a time.
    - Under high message loss rates, clients may have significant delays due to the request processing.
