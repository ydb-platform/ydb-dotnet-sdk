using Google.Protobuf.Collections;
using Xunit;
using Ydb.Issue;
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

        Assert.Equal("Status: Aborted",
            Assert.Throws<YdbException>(() => reader.Read()).Message);
    }

    [Fact]
    public void NextResult_WhenNextResultThrowException_ThrowException()
    {
        var reader = new YdbDataReader(SingleEnumeratorFailed);

        Assert.Equal("Status: Aborted",
            Assert.Throws<YdbException>(() => reader.NextResult()).Message);
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

    [Fact]
    [Trait("Category", "Integration")]
    public async Task CloseAsync_WhenDoubleInvoke_Idempotent()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var ydbCommand = connection.CreateCommand();
        ydbCommand.CommandText = "SELECT 1;";
        var ydbDataReader = ydbCommand.ExecuteReader();

        Assert.True(await ydbDataReader.NextResultAsync());
        await ydbDataReader.CloseAsync();
        await ydbDataReader.CloseAsync();
        Assert.False(await ydbDataReader.NextResultAsync());
    }

    [Fact]
    public void Read_WhenReadAsyncThrowException_AggregateIssuesBeforeErrorAndAfter()
    {
        var result = ResultSet.Parser.ParseJson(
            "{ \"columns\": [ { \"name\": \"column0\", " +
            "\"type\": { \"typeId\": \"BOOL\" } } ], " +
            "\"rows\": [ { \"items\": [ { \"boolValue\": true } ] } ] }"
        );

        var successPart = new ExecuteQueryResponsePart
            { ResultSetIndex = 0, Status = StatusIds.Types.StatusCode.Success, ResultSet = result };
        successPart.Issues.Add(new IssueMessage { Message = "Some message 1" });

        var failPart = new ExecuteQueryResponsePart { Status = StatusIds.Types.StatusCode.Aborted };
        failPart.Issues.Add(new IssueMessage { Message = "Some message 2" });
        failPart.Issues.Add(new IssueMessage { Message = "Some message 2" });

        var nextFailPart = new ExecuteQueryResponsePart { Status = StatusIds.Types.StatusCode.Aborted };
        nextFailPart.Issues.Add(new IssueMessage { Message = "Some message 3" });

        var reader = new YdbDataReader(new MockAsyncEnumerator<ExecuteQueryResponsePart>(
            new List<ExecuteQueryResponsePart> { successPart, failPart, nextFailPart }));

        Assert.True(reader.Read());
        Assert.Equal(@"Status: Aborted, Issues:
[0] Fatal: Some message 1
[0] Fatal: Some message 2
[0] Fatal: Some message 2
[0] Fatal: Some message 3
", Assert.Throws<YdbException>(() => reader.Read()).Message);
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
        new(new List<ExecuteQueryResponsePart> { new() { Status = StatusIds.Types.StatusCode.Aborted } });
}
