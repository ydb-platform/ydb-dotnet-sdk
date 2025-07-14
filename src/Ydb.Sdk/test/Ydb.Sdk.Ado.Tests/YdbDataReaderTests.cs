using Xunit;
using Ydb.Issue;
using Ydb.Query;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests;

public class YdbDataReaderTests
{
    [Fact]
    public async Task BasedIteration_WhenNotCallMethodRead_ThrowException()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(), codes.Add);

        // Read first metadata
        Assert.True(reader.HasRows);
        Assert.Equal(1, reader.FieldCount);
        Assert.Equal("Bool", reader.GetDataTypeName(0));

        Assert.Equal("No row is available", Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);

        Assert.Equal("No row is available",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message); // Need Read()

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));

        Assert.Equal("Ordinal must be between 0 and 0",
            Assert.Throws<IndexOutOfRangeException>(() => reader.GetBoolean(1)).Message);

        Assert.False(reader.Read());
        Assert.False(reader.IsClosed);

        Assert.Equal("No row is available",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);
        Assert.Empty(codes);

        await reader.CloseAsync();
        Assert.True(reader.IsClosed);
        Assert.Equal("The reader is closed",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);
        Assert.Equal("The reader is closed",
            Assert.Throws<InvalidOperationException>(() => reader.Read()).Message);
    }

    [Fact]
    public async Task CreateYdbDataReader_WhenAbortedStatus_ThrowException()
    {
        var codes = new List<StatusCode>();
        Assert.Equal("Status: Aborted", (await Assert.ThrowsAsync<YdbException>(() =>
            YdbDataReader.CreateYdbDataReader(SingleEnumeratorFailed, codes.Add))).Message);
        Assert.Single(codes);
        Assert.Equal(StatusCode.Aborted, codes[0]);
    }

    [Fact]
    public async Task NextResult_WhenNextResultSkipResultSet_ReturnNextResultSet()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2), codes.Add);

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.Read());
        Assert.False(reader.NextResult());
        Assert.Empty(codes);
    }

    [Fact]
    public async Task NextResult_WhenFirstRead_ReturnResultSet()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2), codes.Add);

        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.False(reader.Read());

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.NextResult());
        Assert.False(reader.Read());
        Assert.Empty(codes);
    }

    [Fact]
    public async Task NextResult_WhenLongResultSet_ReturnResultSet()
    {
        var codes = new List<StatusCode>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2, true), codes.Add);

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
        Assert.Empty(codes);
    }

    [Fact]
    public async Task Read_WhenReadAsyncThrowException_AggregateIssuesBeforeErrorAndAfter()
    {
        var codes = new List<StatusCode>();
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

        var reader = await YdbDataReader.CreateYdbDataReader(new MockAsyncEnumerator<ExecuteQueryResponsePart>(
            new List<ExecuteQueryResponsePart> { successPart, failPart, nextFailPart }), codes.Add);

        Assert.True(reader.Read());
        Assert.Equal("""
                     Status: Aborted, Issues:
                     [0] Fatal: Some message 1
                     [0] Fatal: Some message 2
                     [0] Fatal: Some message 2
                     [0] Fatal: Some message 3
                     """, Assert.Throws<YdbException>(() => reader.Read()).Message);
        Assert.Single(codes);
        Assert.Equal(StatusCode.Aborted, codes[0]);
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
