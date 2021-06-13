using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

internal class Actor
{
    private readonly OffsetManager _offsetManager;
    private readonly int _actorIndex;
    private readonly UserStore _userStore;
    private readonly Dictionary<long, User> _users = new();
    private readonly ConcurrentQueue<KafkaAccountOperation> _operationsQueue = new();
    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private Task? _runningTask;

    public Actor(OffsetManager offsetManager, int actorIndex, UserStore userStore)
    {
        _offsetManager = offsetManager;
        _actorIndex = actorIndex;
        _userStore = userStore;
    }
    
    public void EnqueueOperation(KafkaAccountOperation kafkaAccountOperation)
    {
        _operationsQueue.Enqueue(kafkaAccountOperation);
        _offsetManager.MessageLoaded(kafkaAccountOperation.Offset);
    }

    public void StartProcessing()
    {
        _runningTask = Task.Run(ProcessLoopAsync);
    }

    private async Task ProcessLoopAsync()
    {
        while (true)
        {
            _operationsQueue.TryDequeue(out var kafkaAccountOperation);
            if (kafkaAccountOperation != null)
            {
                try
                {
                    await ProcessAsync(kafkaAccountOperation);
                }
                catch (Exception e)
                {
                    // TODO: If processing thrown error
                    // it'll never be able to get into consistent state due to missing message in sequence.

                    Console.WriteLine(e);
                    await Task.Delay(TimeSpan.FromMilliseconds(10));
                }
            }
            else
            {
                if (!_cancellationTokenSource.Token.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromMilliseconds(10));
                }
                else
                {
                    return;
                }
            }
        }
    }

    private async Task ProcessAsync(KafkaAccountOperation kafkaAccountOperation)
    {
        // Get user or create new one
        if (!_users.TryGetValue(kafkaAccountOperation.UserId, out var user))
        {
            var userBalance = await _userStore.GetUserBalanceAsync(kafkaAccountOperation.UserId);
            user = userBalance != null ? new User(userBalance) : new User();

            _users[kafkaAccountOperation.UserId] = user;
        }

        user.UpdateBalance(kafkaAccountOperation, _actorIndex);
        await _userStore.SaveUserBalanceAsync(kafkaAccountOperation.UserId, user.Balance);
        
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