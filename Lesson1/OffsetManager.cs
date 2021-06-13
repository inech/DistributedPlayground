using System;
using System.Collections.Generic;
using System.Linq;

internal class OffsetManager
{
    private readonly object _lock = new();
    private readonly SortedSet<long> _unprocessedOffsets = new();
    private long? _biggestProcessedOffset;
    
    public void MessageLoaded(long offset)
    {
        lock (_lock)
        {
            if (!_unprocessedOffsets.Add(offset))
            {
                Console.WriteLine($"Weird condition. Trying to add offset which already exists {offset}");
            }
        }
    }

    public void MessageProcessed(long offset)
    {
        lock (_lock)
        {
            if (!_unprocessedOffsets.Remove(offset))
            {
                Console.WriteLine($"Weird condition. Trying to remove offset which doesn't exist {offset}");
            }

            _biggestProcessedOffset = _biggestProcessedOffset == null
                ? offset
                : Math.Max(_biggestProcessedOffset.Value, offset);
        }
    }

    // Returns offset which is safe to commit.
    // All messages up to this point should be processed already.
    public long? GetOffsetToCommit()
    {
        lock (_lock)
        {
            if (_unprocessedOffsets.Any())
            {
                return _unprocessedOffsets.First();
            }
            else
            {
                if (_biggestProcessedOffset != null)
                {
                    // All messages have been processed (including _biggestProcessedOffset)
                    // so we can seek to the offset which is greater than biggest that we loaded.
                    return _biggestProcessedOffset.Value + 1;
                }
                else
                {
                    // No messages have been loaded/processed so far.
                    return null;
                }
            }
        }
    }
}