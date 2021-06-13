using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using Shared;

Console.WriteLine("Start");

await EmptyTopicAsync(SharedConstants.TopicName);

ProduceTestData(SharedConstants.TopicName, 2, 4, 10);

static void ProduceTestData(string topicName, int numberOfUsers, int operationsPerUser, int maxComplexity)
{
    var sw = Stopwatch.StartNew();
    
    var producerConfig = new ProducerConfig
    {
        BootstrapServers = SharedConstants.BootstrapServers
    };
    using var producer = new ProducerBuilder<long, AccountOperation>(producerConfig)
        .SetValueSerializer(new JsonSerialization<AccountOperation>())
        .Build();

    var random = new Random();
    var producedCount = 0;
    var acknowledgedCount = 0;
    for (long userId = 1; userId <= numberOfUsers ; userId++)
    {
        for (var sequenceNumber = 1; sequenceNumber <= operationsPerUser; sequenceNumber++)
        {
            var message = new Message<long, AccountOperation>
            {
                Key = userId,
                Value = new AccountOperation(sequenceNumber, random.Next(maxComplexity))
            };
            producer.Produce(topicName, message, report =>
            {
                Interlocked.Increment(ref acknowledgedCount);
                if (report.Error.IsError)
                {
                    throw new Exception($"Error: {report.Error.Reason}");
                }
            });
            var producedSoFar = Interlocked.Increment(ref producedCount);
            if (producedSoFar % 100_000 == 0)
            {
                Console.WriteLine($"Flushing after {producedSoFar} messages...");
                producer.Flush();
            }
        }
    }

    Console.WriteLine("Final flush...");
    producer.Flush();

    sw.Stop();
    
    Console.WriteLine($"Produced {producedCount} messages in {sw.Elapsed}");
    if (producedCount != acknowledgedCount)
    {
        throw new Exception($"Some messages lost. Produced {producedCount} vs. acknowledged {acknowledgedCount}");
    }
}

static async Task EmptyTopicAsync(string topicName)
{
    using var adminClient = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = SharedConstants.BootstrapServers })
        .Build();

    Console.WriteLine("Starting emptying topic");

    var clusterMetadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10));
    var topicMetadata = clusterMetadata.Topics.Single();
    foreach (var partitionMetadata in topicMetadata.Partitions)
    {
        var topicOffsetPartition = new TopicPartitionOffset(topicName, partitionMetadata.PartitionId, Offset.End);
        await adminClient.DeleteRecordsAsync(new[] {topicOffsetPartition}, new DeleteRecordsOptions
        {
            OperationTimeout = TimeSpan.FromSeconds(5),
            RequestTimeout= TimeSpan.FromSeconds(5)
        });
        Console.WriteLine($"Emptied partition {partitionMetadata.PartitionId}");
    }
    
    Console.WriteLine("Topic emptied");
}