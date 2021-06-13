using System;
using System.Threading;

internal class User
{
    public User()
    {
        Balance = new (0, -1, DateTime.MinValue);
    }

    public User(UserBalance balance)
    {
        Balance = balance;
    }
    
    public UserBalance Balance { get; private set; }

    // Returns whether was updated (true) or replay
    public bool UpdateBalance(KafkaAccountOperation kafkaAccountOperation, int actorIndex)
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
            
            Balance = new UserBalance(kafkaAccountOperation.Operation.SequenceNumber, kafkaAccountOperation.Offset, DateTime.UtcNow);
            
            Console.WriteLine($"[{DateTime.UtcNow:hh:mm:ss:fff}] [Verbose] Processed Actor({actorIndex:000}) U({kafkaAccountOperation.UserId:000}) SN({kafkaAccountOperation.Operation.SequenceNumber:000}) KafkaOffset({kafkaAccountOperation.Offset:000000})");

            return true;
        }
        else
        {
            Console.WriteLine($"Idempotency guard: replay detected. Balance Kafka Offset {Balance.KafkaOffset}, " +
                              $"received offset {kafkaAccountOperation.Offset}.");
            return false;
        }
    }
}