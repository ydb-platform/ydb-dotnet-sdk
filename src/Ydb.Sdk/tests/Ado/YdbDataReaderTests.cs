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
        var reader = new YdbDataReader(EnumeratorSuccess());

        Assert.Equal("Invalid attempt to read when no data is present",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);

        Assert.True(reader.NextResult());

        Assert.Equal("Invalid attempt to read when no data is present",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message); // Need Read()

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));
        Assert.Equal("Bool", reader.GetDataTypeName(0));

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

    [Fact]
    public void NextResult_WhenNextResultSkipResultSet_ReturnNextResultSet()
    {
        var reader = new YdbDataReader(EnumeratorSuccess(2));

        Assert.True(reader.NextResult());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.Read());
        Assert.False(reader.NextResult());
    }

    [Fact]
    public void NextResult_WhenFirstRead_ReturnResultSet()
    {
        var reader = new YdbDataReader(EnumeratorSuccess(2));

        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.False(reader.Read());

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.NextResult());
        Assert.False(reader.Read());
    }

    [Fact]
    public void NextResult_WhenLongResultSet_ReturnResultSet()
    {
        var reader = new YdbDataReader(EnumeratorSuccess(2, true));

        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.False(reader.Read());

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.NextResult());
        Assert.False(reader.Read());
    }

    private static MockAsyncEnumerator<ExecuteQueryResponsePart> EnumeratorSuccess(int size = 1,
        bool longFirstResultSet = false)
    {
        var result = ResultSet.Parser.ParseJson(
            "{ \"columns\": [ { \"name\": \"column0\", " +
            "\"type\": { \"typeId\": \"BOOL\" } } ], " +
            "\"rows\": [ { \"items\": [ { \"boolValue\": true } ] } ] }"
        );

        var list = new List<ExecuteQueryResponsePart>();

        if (longFirstResultSet)
        {
            list.Add(new ExecuteQueryResponsePart
                { ResultSetIndex = 0, Status = StatusIds.Types.StatusCode.Success, ResultSet = result });
        }

        for (var i = 0; i < size; i++)
        {
            list.Add(new ExecuteQueryResponsePart
                { ResultSetIndex = i, Status = StatusIds.Types.StatusCode.Success, ResultSet = result });
        }

        return new MockAsyncEnumerator<ExecuteQueryResponsePart>(list);
    }

    private static MockAsyncEnumerator<ExecuteQueryResponsePart> SingleEnumeratorFailed =>
        new(new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.NotFound } });
}
