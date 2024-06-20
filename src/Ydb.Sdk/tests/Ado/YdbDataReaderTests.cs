using Xunit;
using Ydb.Query;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Unit")]
public class YdbDataReaderTests
{
    [Fact]
    public void BasedIteration_WhenNotCallMethodRead_ThrowException()
    {
        var reader = new YdbDataReader(SingleEnumeratorSuccess);

        Assert.Equal("Invalid attempt to read when no data is present",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);

        Assert.True(reader.NextResult());

        Assert.Equal("Invalid attempt to read when no data is present",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message); // Need Read()

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));

        Assert.Equal("Ordinal must be between 0 and 0",
            Assert.Throws<IndexOutOfRangeException>(() => reader.GetBoolean(1)).Message);

        Assert.True(reader.HasRows);
        Assert.Equal(1, reader.FieldCount);
        Assert.False(reader.Read());
        Assert.True(reader.IsClosed);

        Assert.Equal("The reader is closed",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);
    }

    [Fact]
    public void Read_WhenNextResultThrowException_ThrowException()
    {
        var reader = new YdbDataReader(SingleEnumeratorFailed);

        Assert.Equal("Unable to read data from the transport connection",
            Assert.Throws<YdbAdoException>(() => reader.Read()).Message);
    }

    [Fact]
    public void NextResult_WhenNextResultThrowException_ThrowException()
    {
        var reader = new YdbDataReader(SingleEnumeratorFailed);

        Assert.Equal("Unable to read data from the transport connection",
            Assert.Throws<YdbAdoException>(() => reader.NextResult()).Message);
    }

    private static MockAsyncEnumerator<ExecuteQueryResponsePart> SingleEnumeratorSuccess
    {
        get
        {
            var result = ResultSet.Parser.ParseJson(
                "{ \"columns\": [ { \"name\": \"column0\", " +
                "\"type\": { \"typeId\": \"BOOL\" } } ], " +
                "\"rows\": [ { \"items\": [ { \"boolValue\": true } ] } ] }"
            );

            return new MockAsyncEnumerator<ExecuteQueryResponsePart>(new List<ExecuteQueryResponsePart>
                { new() { Status = StatusIds.Types.StatusCode.Success, ResultSet = result } });
        }
    }

    private static MockAsyncEnumerator<ExecuteQueryResponsePart> SingleEnumeratorFailed =>
        new(new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.NotFound } });
}
