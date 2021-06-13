using System;
using System.Diagnostics;
using Confluent.Kafka;
using Shared;

Console.WriteLine("Program started");

Consume(SharedConstants.TopicName, DateTime.UtcNow.ToFileTimeUtc().ToString());

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
    while (true)
    {
        var result = consumer.Consume();
        if (result.IsPartitionEOF)
        {
            break;
        }
        
        messageCount++;
        
        var (userId, accountOperation) = (result.Message.Key, result.Message.Value);
        Console.WriteLine($"#{messageCount}: U({userId}) SN({accountOperation.SequenceNumber})");
    }
    
    consumer.Close();

    sw.Stop();
    Console.WriteLine($"Consumed {messageCount} messages in {sw.Elapsed}");
}

