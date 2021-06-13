using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Shared;

Console.WriteLine("Program started");

await ConsumeAsync(SharedConstants.TopicName, DateTime.UtcNow.ToFileTimeUtc().ToString(), 10);

Console.WriteLine("Program Finished");

static async Task ConsumeAsync(string topicName, string groupId, int concurrency)
{
    var sw = Stopwatch.StartNew();
    
    using var consumer = new ConsumerBuilder<long, AccountOperation>(new ConsumerConfig
        {
            BootstrapServers = SharedConstants.BootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnablePartitionEof = true,
            EnableAutoOffsetStore = false
        })
        .SetValueDeserializer(new JsonSerialization<AccountOperation>())
        .SetErrorHandler((_, e) =>
        {
            Console.WriteLine($"Error: {e.Reason}");
        })
        .Build();
    consumer.Subscribe(topicName);

    var distributor = new Distributor(concurrency);
    distributor.StartProcessing(consumer, topicName);

    Console.WriteLine("Processing started");
    
    while (true)
    {
        var result = consumer.Consume();
        if (result.IsPartitionEOF)
        {
            Console.WriteLine("Consumed all messages");
            break;
        }
        
        var operation = new KafkaAccountOperation(result.Message.Key, result.Message.Value, result.Offset);
        distributor.Distribute(operation);
    }
    
    Console.WriteLine("Waiting to finish processing and stop gracefully...");
    await distributor.StopGracefullyAsync();
    
    consumer.Close();

    distributor.PrintResult();
    
    sw.Stop();
    Console.WriteLine($"Finished in {sw.Elapsed}");
}

internal class OffsetManager
{
    private readonly object _lock = new();
    private readonly SortedSet<long> _unprocessedOffsets = new();
    private long? _biggestProcessedOffset;
    
    public void MessageLoaded(long offset)
    {
        lock (_lock)
        {
            if (!_unprocessedOffsets.Add(offset))
            {
                Console.WriteLine($"Weird condition. Trying to add offset which already exists {offset}");
            }
        }
    }

    public void MessageProcessed(long offset)
    {
        lock (_lock)
        {
            if (!_unprocessedOffsets.Remove(offset))
            {
                Console.WriteLine($"Weird condition. Trying to remove offset which doesn't exist {offset}");
            }

            _biggestProcessedOffset = _biggestProcessedOffset == null
                ? offset
                : Math.Max(_biggestProcessedOffset.Value, offset);
        }
    }

    // Returns offset which is safe to commit.
    // All messages up to this point should be processed already.
    public long? GetOffsetToCommit()
    {
        lock (_lock)
        {
            if (_unprocessedOffsets.Any())
            {
                return _unprocessedOffsets.First();
            }
            else
            {
                if (_biggestProcessedOffset != null)
                {
                   // All messages have been processed (including _biggestProcessedOffset)
                   // so we can seek to the offset which is greater than biggest that we loaded.
                   return _biggestProcessedOffset.Value + 1;
                }
                else
                {
                    // No messages have been loaded/processed so far.
                    return null;
                }
            }
        }
    }
}

internal class Distributor
{
    private readonly Actor[] _actors;
    private readonly OffsetManager _offsetManager = new();
    private readonly CancellationTokenSource _ctsStoreOffset = new();
    private Task _storeOffsetTask;
    private long? _informationLastStoredOffset;

    public Distributor(int concurrency)
    {
        _actors = Enumerable.Range(0, concurrency).Select(i => new Actor(_offsetManager, i)).ToArray();
    }

    public void Distribute(KafkaAccountOperation kafkaAccountOperation)
    {
        var actorIndex = kafkaAccountOperation.UserId % _actors.Length; // In production would be uniform hashing algo
        _actors[actorIndex].EnqueueOperation(kafkaAccountOperation);
    }

    public void StartProcessing(IConsumer<long, AccountOperation> kafkaConsumer, string topicName)
    {
        _storeOffsetTask = Task.Factory.StartNew(
            () => StoreOffsetLoop(kafkaConsumer, topicName),
            TaskCreationOptions.LongRunning);
        
        foreach (var actor in _actors)
        {
            actor.StartProcessing();
        }
    }

    // TODO: Poor separation
    private void StoreOffsetLoop(IConsumer<long, AccountOperation> kafkaConsumer, string topicName)
    {
        try
        {
            while (!_ctsStoreOffset.IsCancellationRequested)
            {
                StoreOffset(kafkaConsumer, topicName);
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
        
            // Store final time to avoid race conditions
            StoreOffset(kafkaConsumer, topicName);
        }
        catch (Exception e)
        {
            Console.WriteLine($"StoreOffset crashed {e}");
            throw;
        }
    }

    private void StoreOffset(IConsumer<long, AccountOperation> kafkaConsumer, string topicName)
    {
        var offsetToCommit = _offsetManager.GetOffsetToCommit();
        if (offsetToCommit != null)
        {
            kafkaConsumer.StoreOffset(new TopicPartitionOffset(topicName, new Partition(0), offsetToCommit.Value)); // TODO: Only single partition

            Console.WriteLine($"[{DateTime.UtcNow:hh:mm:ss:fff}] [Verbose] Store offset ({offsetToCommit:000000})");
        }
        
        _informationLastStoredOffset = offsetToCommit;
    }

    public async Task StopGracefullyAsync()
    {
        await Task.WhenAll(_actors
            .Select(a => a.FinishQueueAndStopAsync())
            .ToArray()
        );

        _ctsStoreOffset.Cancel();
        await _storeOffsetTask;
    }

    public void PrintResult()
    {
        Console.WriteLine($"Last stored offset: {_informationLastStoredOffset}");
        foreach (var actor in _actors)
        {
            actor.PrintResult();
        }
    }
}

internal class Actor
{
    private readonly OffsetManager _offsetManager;
    private readonly int _actorIndex;
    private readonly Dictionary<long, User> _users = new();
    private readonly ConcurrentQueue<KafkaAccountOperation> _operationsQueue = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private Task? _runningTask;

    public Actor(OffsetManager offsetManager, int actorIndex)
    {
        _offsetManager = offsetManager;
        _actorIndex = actorIndex;
    }
    
    public void EnqueueOperation(KafkaAccountOperation kafkaAccountOperation)
    {
        _operationsQueue.Enqueue(kafkaAccountOperation);
        _offsetManager.MessageLoaded(kafkaAccountOperation.Offset);
    }

    public void StartProcessing()
    {
        _runningTask = Task.Factory.StartNew(ProcessLoop, TaskCreationOptions.LongRunning);
    }

    private void ProcessLoop()
    {
        while (true)
        {
            _operationsQueue.TryDequeue(out var kafkaAccountOperation);
            if (kafkaAccountOperation != null)
            {
                try
                {
                    Process(kafkaAccountOperation);
                }
                catch (Exception e)
                {
                    // TODO: If processing thrown error
                    // it'll never be able to get into consistent state due to missing message in sequence.

                    Console.WriteLine(e);
                    Thread.Sleep(TimeSpan.FromMilliseconds(10));
                    
                }
            }
            else
            {
                if (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    Thread.Sleep(TimeSpan.FromMilliseconds(10));
                }
                else
                {
                    return;
                }
            }
        }
    }

    private void Process(KafkaAccountOperation kafkaAccountOperation)
    {
        // Get user or create new one
        if (!_users.TryGetValue(kafkaAccountOperation.UserId, out var user))
        {
            _users[kafkaAccountOperation.UserId] = user = new User();
        }

        user.UpdateBalance(kafkaAccountOperation, _actorIndex);
        
        _offsetManager.MessageProcessed(kafkaAccountOperation.Offset);
    }

    public async Task FinishQueueAndStopAsync()
    {
        if (_runningTask == null)
        {
            throw new Exception("Actor is not running.");
        }
        
        _cancellationTokenSource.Cancel();
        await _runningTask;
    }

    public void PrintResult()
    {
        Console.WriteLine($"Actor results:");
        foreach (var (userId, user) in _users.OrderBy(kv => kv.Key))
        {
            Console.WriteLine($"U({userId}) SN({user.Balance.SequenceNumber}) K({user.Balance.KafkaOffset})");
        }
    }
}

internal class User
{
    public UserBalance Balance { get; private set; } = new (0, -1);

    // Returns last offset
    public void UpdateBalance(KafkaAccountOperation kafkaAccountOperation, int actorIndex)
    {
        if (kafkaAccountOperation.Offset > Balance.KafkaOffset)
        {
            var expectedSequenceNumber = Balance.SequenceNumber + 1;
            if (kafkaAccountOperation.Operation.SequenceNumber != expectedSequenceNumber)
            {
                throw new InconsistencyException(
                    $"Expected to have sequence {expectedSequenceNumber} but instead {kafkaAccountOperation.Operation.SequenceNumber}");
            }
            
            // Simulate heavy operations
            Thread.Sleep(TimeSpan.FromSeconds(kafkaAccountOperation.Operation.Complexity));
            
            Balance = new UserBalance(kafkaAccountOperation.Operation.SequenceNumber, kafkaAccountOperation.Offset);
            
            Console.WriteLine($"[{DateTime.UtcNow:hh:mm:ss:fff}] [Verbose] Processed Actor({actorIndex:000}) U({kafkaAccountOperation.UserId:000}) SN({kafkaAccountOperation.Operation.SequenceNumber:000}) KafkaOffset({kafkaAccountOperation.Offset:000000})");
        }
        else
        {
            Console.WriteLine($"Idempotency guard: replay detected. Balance Kafka Offset {Balance.KafkaOffset}, " +
                              $"received offset {kafkaAccountOperation.Offset}.");
        }
    }
}

internal record UserBalance(int SequenceNumber, long KafkaOffset); // TODO: Kafka offset when multiple partitions with re-balance?

internal record KafkaAccountOperation(long UserId, AccountOperation Operation, long Offset);

internal class InconsistencyException : Exception
{
    public InconsistencyException(string message) : base(message)
    {
    }
}
