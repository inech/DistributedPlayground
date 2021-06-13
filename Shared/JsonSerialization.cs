using System;
using System.Text.Json;
using Confluent.Kafka;

namespace Shared
{
    public class JsonSerialization<T> : ISerializer<T>, IDeserializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context)
        {
            if (data == null) throw new ArgumentNullException(nameof(data));
            
            return JsonSerializer.SerializeToUtf8Bytes(data);
        }

        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull)
            {
                throw new Exception("Null messages not allowed.");
            }

            return JsonSerializer.Deserialize<T>(data);
        }
    }
}