using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Confluent.Kafka;
using Shared;

Console.WriteLine("Program started");

Consume(SharedConstants.TopicName, DateTime.UtcNow.ToFileTimeUtc().ToString());

Console.WriteLine("Program Finished");

static void Consume(string topicName, string groupId)
{
    var sw = Stopwatch.StartNew();
    
    var consumerConfig = new ConsumerConfig
    {
        BootstrapServers = SharedConstants.BootstrapServers,
        GroupId = groupId,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnablePartitionEof = true
    };

    using var consumer = new ConsumerBuilder<long, AccountOperation>(consumerConfig)
        .SetValueDeserializer(new JsonSerialization<AccountOperation>())
        .SetErrorHandler((_, e) =>
        {
            Console.WriteLine($"Error: {e.Reason}");
        })
        .Build();
    consumer.Subscribe(topicName);

    var messageCount = 0;

    var users = new Dictionary<long, User>();
    while (true)
    {
        var result = consumer.Consume();
        if (result.IsPartitionEOF)
        {
            break;
        }
        
        messageCount++;

        var userId = result.Message.Key;
        var kafkaAccountOperation = new KafkaAccountOperation(result.Message.Value, result.Offset);
        Console.WriteLine($"#{messageCount}: U({userId}) SN({kafkaAccountOperation.Operation.SequenceNumber})");

        // Get user or create new one
        if (!users.TryGetValue(userId, out var user))
        {
            users[userId] = user = new User();
        }
        
        user.UpdateBalance(kafkaAccountOperation);
    }
    
    consumer.Close();

    sw.Stop();
    Console.WriteLine($"Consumed {messageCount} messages in {sw.Elapsed}");

    Console.WriteLine("Printing result:");
    foreach (var userId in users.Keys.OrderBy(id => id))
    {
        var user = users[userId];
        Console.WriteLine($"U({userId}) SN({user.Balance.SequenceNumber}) K({user.Balance.KafkaOffset})");
    }
}

internal class User
{
    public UserBalance Balance { get; private set; } = new UserBalance(0, -1);

    public void UpdateBalance(KafkaAccountOperation kafkaAccountOperation)
    {
        var (accountOperation, offset) = kafkaAccountOperation;
        
        if (offset > Balance.KafkaOffset)
        {
            var expectedSequenceNumber = Balance.SequenceNumber + 1;
            if (accountOperation.SequenceNumber != expectedSequenceNumber)
            {
                throw new InconsistencyException(
                    $"Expected to have sequence {expectedSequenceNumber} but instead {accountOperation.SequenceNumber}");
            }
            
            Balance = new UserBalance(accountOperation.SequenceNumber, offset);
        }
        else
        {
            Console.WriteLine($"Idempotency guard: replay detected. Balance Kafka Offset {Balance.KafkaOffset}, " +
                              $"received offset {offset}.");
        }
    }

    private readonly Queue<KafkaAccountOperation> _pendingKafkaAccountOperations = new();

    public void EnqueueOperation(KafkaAccountOperation kafkaAccountOperation)
    {
        _pendingKafkaAccountOperations.Enqueue(kafkaAccountOperation);
    }
}

internal record UserBalance(int SequenceNumber, long KafkaOffset); // TODO: Kafka offset when multiple partitions with re-balance?

internal record KafkaAccountOperation(AccountOperation Operation, long Offset);

internal class InconsistencyException : Exception
{
    public InconsistencyException(string message) : base(message)
    {
    }
}
