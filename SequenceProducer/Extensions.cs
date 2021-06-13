using System;
using System.Collections.Generic;
using System.Linq;

namespace SequenceProducer
{
    public static class Extensions
    {
        private static readonly Random Random = new();

        public static IEnumerable<T> Shuffle<T>(this IEnumerable<T> sequence) => sequence.OrderBy(_ => Random.Next());
    }
}