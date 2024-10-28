using System.Text;

namespace Ydb.Sdk.Services.Topic;

public interface IDeserializer<out TValue>
{
    /// <summary>Deserialize a message key or value.</summary>
    /// <param name="data">The data to deserialize.</param>
    /// <returns>The deserialized value.</returns>
    TValue Deserialize(byte[] data);
}

public static class Deserializers
{
    /// <summary>
    /// String (UTF8 encoded) deserializer.
    /// </summary>
    public static IDeserializer<string> Utf8 = new Utf8Deserializer();

    /// <summary>
    /// System.Int64 (big endian encoded, network byte ordered) deserializer.
    /// </summary>
    public static IDeserializer<long> Int64 = new Int64Deserializer();

    /// <summary>
    /// System.Int32 (big endian encoded, network byte ordered) deserializer.
    /// </summary>
    public static IDeserializer<int> Int32 = new Int32Deserializer();

    /// <summary>
    /// System.Byte[] deserializer.
    /// </summary>
    /// <remarks>
    /// Byte ordering is original order.
    /// </remarks>
    public static IDeserializer<byte[]> ByteArray = new ByteArrayDeserializer();

    internal static readonly Dictionary<System.Type, object> DefaultSerializers = new()
    {
        { typeof(int), Int32 },
        { typeof(long), Int64 },
        { typeof(string), Utf8 },
        { typeof(byte[]), ByteArray }
    };

    private class Utf8Deserializer : IDeserializer<string>
    {
        public string Deserialize(byte[] data)
        {
            return Encoding.UTF8.GetString(data);
        }
    }

    private class Int64Deserializer : IDeserializer<long>
    {
        public long Deserialize(byte[] data)
        {
            if (data.Length != 8)
            {
                throw new ArgumentException(
                    $"Deserializer<Long> encountered data of length ${data.Length}. Expecting data length to be 8");
            }

            return ((long)data[0] << 56) | ((long)data[1] << 48) | ((long)data[2] << 40) | ((long)data[3] << 32) |
                   ((long)data[4] << 24) | ((long)data[5] << 16) | ((long)data[6] << 8) | data[7];
        }
    }

    private class Int32Deserializer : IDeserializer<int>
    {
        public int Deserialize(byte[] data)
        {
            if (data.Length != 4)
            {
                throw new ArgumentException(
                    $"Deserializer<Int32> encountered data of length ${data.Length}. Expecting data length to be 4");
            }

            return (data[0] << 24) | (data[1] << 16) | (data[2] << 8) | data[3];
        }
    }

    private class ByteArrayDeserializer : IDeserializer<byte[]>
    {
        public byte[] Deserialize(byte[] data)
        {
            return data;
        }
    }
}
