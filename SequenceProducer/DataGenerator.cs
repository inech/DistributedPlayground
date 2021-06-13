using System;
using System.Collections.Generic;
using System.Linq;
using Shared;

namespace SequenceProducer
{
    public static class DataGenerator
    {
        private static readonly Random Random = new();
        
        public static IEnumerable<UserAccountOperation> GenerateUserAccountOperations(
            int numberOfUsers, int operationsPerUser, int maxComplexity)
        {
            var queuePerUser = Enumerable
                .Range(1, numberOfUsers)
                .ToDictionary(
                    userId => userId,
                    _ => CreateAccountOperationQueue(operationsPerUser, maxComplexity));
            var peekRandomUserSequence = Enumerable
                .Range(1, numberOfUsers)
                .SelectMany(userId => Enumerable.Repeat(userId, operationsPerUser)) // Peek this user X times
                .Shuffle();
            foreach (var userId in peekRandomUserSequence)
            {
                var queue = queuePerUser[userId];
                var accountOperation = queue.Dequeue();
                yield return new UserAccountOperation(userId, accountOperation);
            }
        }

        private static Queue<AccountOperation> CreateAccountOperationQueue(int count, int maxComplexity) =>
            new (Enumerable
                .Range(1, count)
                .Select(s => new AccountOperation(s, Random.Next(maxComplexity)))
            );
    }
}