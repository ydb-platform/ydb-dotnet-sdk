using System.Text;

namespace Ydb.Sdk.Services.Topic;

public interface ISerializer<in TValue>
{
    public byte[] Serialize(TValue data);
}

public static class Serializers
{
    /// <summary>String (UTF8) serializer.</summary>
    public static readonly ISerializer<string> Utf8 = new Utf8Serializer();

    /// <summary>
    /// System.Int64 (big endian, network byte order) serializer.
    /// </summary>
    public static readonly ISerializer<long> Int64 = new Int64Serializer();

    /// <summary>
    /// System.Int32 (big endian, network byte order) serializer.
    /// </summary>
    public static readonly ISerializer<int> Int32 = new Int32Serializer();

    /// <summary>
    /// System.Byte[] serializer.</summary>
    /// <remarks>
    /// Byte order is original order.
    /// </remarks>
    public static readonly ISerializer<byte[]> ByteArray = new ByteArraySerializer();

    internal static readonly Dictionary<System.Type, object> DefaultSerializers = new()
    {
        { typeof(int), Int32 },
        { typeof(long), Int64 },
        { typeof(string), Utf8 },
        { typeof(byte[]), ByteArray }
    };

    private class Utf8Serializer : ISerializer<string>
    {
        public byte[] Serialize(string data)
        {
            return Encoding.UTF8.GetBytes(data);
        }
    }

    private class Int64Serializer : ISerializer<long>
    {
        public byte[] Serialize(long data)
        {
            return new[]
            {
                (byte)(data >> 56),
                (byte)(data >> 48),
                (byte)(data >> 40),
                (byte)(data >> 32),
                (byte)(data >> 24),
                (byte)(data >> 16),
                (byte)(data >> 8),
                (byte)data
            };
        }
    }

    private class Int32Serializer : ISerializer<int>
    {
        public byte[] Serialize(int data)
        {
            return new[]
            {
                (byte)(data >> 24),
                (byte)(data >> 16),
                (byte)(data >> 8),
                (byte)data
            };
        }
    }

    private class ByteArraySerializer : ISerializer<byte[]>
    {
        public byte[] Serialize(byte[] data)
        {
            return data;
        }
    }
}
