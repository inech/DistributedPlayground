using System;

internal record UserBalance(int SequenceNumber, long KafkaOffset, DateTime LastOperationTime);