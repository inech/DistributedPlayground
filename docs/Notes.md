# Producer
Producer - 3.5 Pomodoro

# Consumer
0. Planning -> 1 Pomodoro
1. Implement reading sequence operations in 1 thread -> 1=2 Pomodoro
    * Boilerplate: create consumer (first P)
        * reset consumer group - a bit complicated (?)
    * Create internal dictionary (Key - user, Value - Sequence number) YET
        * AutoCommit offset for now
        * Add validation that sequence matches
        * Print out Dictionary when kafka sequence ends ?
2. Implement pump 2 = 6.5 Pomodoro
    * Pump Reads messages into internal buffer which stores
        * Actual messages to be distributed
        * Uncommited offsets: Structure with all offsets read by consumer
    * N number of threads take messages from pump for their own users (hashing of sorts):
        * Store them into dictionary (if offset is newer than what's saved - at least once semantics)
        * Delete offsets from "uncommited offsets"
    * Background commiter threads:
        * Every X seconds commit smallest offset from the structure
        * Flush functionality?

# Edge cases
1. Backpressure not to have too many loaded messages (uncommited offsets)? (C# consumer/producer, dataflow, kafka own mechanism for how long to read?)
2. What if offset gets reset back on the Kafka server?
3. Is it possible for a message with a given key to appear in another partition?
4. Take into account that it's "At least once" so storage should be idempotent

# Improve ideas
1. Consider TPL Dataflow / C# Consumer-producer / Actor framework

# Storing to database
1. File per user:
    * Store latest info (but keep all previous states for ease of debugging)
2. When reloaded read latest state from disk
3. Delete files when full reload
4. Producer - additional option to start from Seq X
0. Edge cases:
    * No error handling when fails writing to disc
    * No fail safety from file corruption?