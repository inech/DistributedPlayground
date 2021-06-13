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

await ConsumeAsync(SharedConstants.TopicName, DateTime.UtcNow.ToFileTimeUtc().ToString(), 2);

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
    distributor.StartProcessing();

    Console.WriteLine("Processing started");
    
    while (true)
    {
        var result = consumer.Consume();
        if (result.IsPartitionEOF)
        {
            Console.WriteLine("End of partition reached");
            break;
        }
        
        var operation = new KafkaAccountOperation(result.Message.Key, result.Message.Value, result.Offset);
        distributor.Distribute(operation);
    }
    
    consumer.Close();

    await distributor.StopGracefullyAsync();
    
    distributor.PrintResult();
    
    sw.Stop();
    Console.WriteLine($"Finished in {sw.Elapsed}");
}

internal class Distributor
{
    private readonly Actor[] _actors;

    public Distributor(int concurrency)
    {
        _actors = Enumerable.Range(0, concurrency).Select(_ => new Actor()).ToArray();
    }

    public void Distribute(KafkaAccountOperation kafkaAccountOperation)
    {
        var actorIndex = kafkaAccountOperation.UserId % _actors.Length; // In production would be uniform hashing algo
        _actors[actorIndex].EnqueueOperation(kafkaAccountOperation);
    }

    public void StartProcessing()
    {
        foreach (var actor in _actors)
        {
            actor.StartProcessing();
        }
    }

    public async Task StopGracefullyAsync()
    {
        await Task.WhenAll(_actors
            .Select(a => a.FinishQueueAndStopAsync())
            .ToArray()
        );
    }

    public void PrintResult()
    {
        foreach (var actor in _actors)
        {
            actor.PrintResult();
        }
    }
}

internal class Actor
{
    private readonly Dictionary<long, User> _users = new();
    private readonly ConcurrentQueue<KafkaAccountOperation> _operationsQueue = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private Task? _runningTask;

    public void EnqueueOperation(KafkaAccountOperation kafkaAccountOperation) => 
        _operationsQueue.Enqueue(kafkaAccountOperation);
    
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

        user.UpdateBalance(kafkaAccountOperation);

        // TODO: Remove pending offset
        // lock (pendingOffsets)
        // {
        //     pendingOffsets.Remove(kafkaAccountOperation.Offset);
        // }
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
    public UserBalance Balance { get; private set; } = new UserBalance(0, -1);

    public void UpdateBalance(KafkaAccountOperation kafkaAccountOperation)
    {
        if (kafkaAccountOperation.Offset > Balance.KafkaOffset)
        {
            var expectedSequenceNumber = Balance.SequenceNumber + 1;
            if (kafkaAccountOperation.Operation.SequenceNumber != expectedSequenceNumber)
            {
                throw new InconsistencyException(
                    $"Expected to have sequence {expectedSequenceNumber} but instead {kafkaAccountOperation.Operation.SequenceNumber}");
            }
            
            // Simulate heavy operation
            Thread.Sleep(TimeSpan.FromSeconds(kafkaAccountOperation.Operation.Complexity) / 2);
            
            Balance = new UserBalance(kafkaAccountOperation.Operation.SequenceNumber, kafkaAccountOperation.Offset);
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
