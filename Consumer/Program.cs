using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Confluent.Kafka;
using Shared;

//var groupId = DateTime.UtcNow.ToFileTimeUtc().ToString();
var groupId = "test";

Console.WriteLine($"Program started for group {groupId}");

await ConsumeAsync(SharedConstants.TopicName, groupId, 10);

Console.WriteLine($"Program Finished for group {groupId}");

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

    var userStore = new UserStore(Path.Combine("users", groupId));
    var distributor = new Distributor(concurrency, userStore);
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

// TODO: Kafka offset when multiple partitions with re-balance?

internal record KafkaAccountOperation(long UserId, AccountOperation Operation, long Offset);

internal class InconsistencyException : Exception
{
    public InconsistencyException(string message) : base(message)
    {
    }
}
