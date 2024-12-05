using Xunit;
using Ydb.Sdk.Services.Topic;

namespace Ydb.Sdk.Tests.Topic;

public class SerializerDeserializerUnitTests
{
    [Fact]
    public void SerializeDeserialize_WhenSerializer64Deserializer32_ReturnInt32()
    {
        Assert.Equal(32, Deserializers.Int32.Deserialize(Serializers.Int32.Serialize(32)));
    }

    [Fact]
    public void SerializeDeserialize_WhenSerializer64Deserializer64_ReturnInt64()
    {
        Assert.Equal(32 * 1_000_000_000L,
            Deserializers.Int64.Deserialize(Serializers.Int64.Serialize(32 * 1_000_000_000L)));
    }

    [Fact]
    public void SerializeDeserialize_WhenSerializerUtf8DeserializerUtf8_ReturnString()
    {
        Assert.Equal("abacaba",
            Deserializers.Utf8.Deserialize(Serializers.Utf8.Serialize("abacaba")));
    }
}
