using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Shared;

internal class Distributor
{
    private readonly Actor[] _actors;
    private readonly OffsetManager _offsetManager = new();
    private readonly CancellationTokenSource _ctsStoreOffset = new();
    private Task _storeOffsetTask;
    private long? _informationLastStoredOffset;

    public Distributor(int concurrency, UserStore userStore)
    {
        _actors = Enumerable.Range(0, concurrency).Select(i => new Actor(_offsetManager, i, userStore)).ToArray();
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