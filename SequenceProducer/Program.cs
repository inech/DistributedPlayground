using System;
using SequenceProducer;
using Shared;

Console.WriteLine("Program started");

await KafkaUtils.EmptyTopicAsync(SharedConstants.TopicName);

var userAccountOperations = 
    DataGenerator.GenerateUserAccountOperations(5, 10, 10);

KafkaUtils.ProduceTestData(SharedConstants.TopicName, userAccountOperations);

Console.WriteLine("Program finished");