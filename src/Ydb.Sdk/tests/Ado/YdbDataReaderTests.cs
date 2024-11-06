using Xunit;
using Ydb.Issue;
using Ydb.Query;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

public class YdbDataReaderTests
{
    [Fact]
    public async Task BasedIteration_WhenNotCallMethodRead_ThrowException()
    {
        var statuses = new List<Status>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(), statuses.Add);

        // Read first metadata
        Assert.True(reader.HasRows);
        Assert.Equal(1, reader.FieldCount);
        Assert.Equal("Bool", reader.GetDataTypeName(0));

        Assert.Equal("No row is available", Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);

        Assert.True(reader.NextResult());

        Assert.Equal("No row is available",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message); // Need Read()

        Assert.True(reader.Read());
        Assert.True(reader.GetBoolean(0));

        Assert.Equal("Ordinal must be between 0 and 0",
            Assert.Throws<IndexOutOfRangeException>(() => reader.GetBoolean(1)).Message);

        Assert.False(reader.Read());
        Assert.True(reader.IsClosed);

        Assert.Equal("The reader is closed",
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0)).Message);
        Assert.Empty(statuses);
    }

    [Fact]
    public void CreateYdbDataReader_WhenAbortedStatus_ThrowException()
    {
        var statuses = new List<Status>();
        Assert.Equal("Status: Aborted", Assert.Throws<YdbException>(
                () => YdbDataReader.CreateYdbDataReader(SingleEnumeratorFailed, statuses.Add).GetAwaiter().GetResult())
            .Message);
        Assert.Single(statuses);
        Assert.Equal(StatusCode.Aborted, statuses[0].StatusCode);
    }

    [Fact]
    public async Task NextResult_WhenNextResultSkipResultSet_ReturnNextResultSet()
    {
        var statuses = new List<Status>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2), statuses.Add);

        Assert.True(reader.NextResult());
        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.Read());
        Assert.False(reader.NextResult());
        Assert.Empty(statuses);
    }

    [Fact]
    public async Task NextResult_WhenFirstRead_ReturnResultSet()
    {
        var statuses = new List<Status>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2), statuses.Add);

        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));
        Assert.False(reader.Read());

        Assert.True(reader.NextResult());
        Assert.True(reader.Read());
        Assert.True((bool)reader.GetValue(0));

        Assert.False(reader.NextResult());
        Assert.False(reader.Read());
        Assert.Empty(statuses);
    }

    [Fact]
    public async Task NextResult_WhenLongResultSet_ReturnResultSet()
    {
        var statuses = new List<Status>();
        var reader = await YdbDataReader.CreateYdbDataReader(EnumeratorSuccess(2, true), statuses.Add);

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
        Assert.Empty(statuses);
    }

    [Fact]
    public async Task Read_WhenReadAsyncThrowException_AggregateIssuesBeforeErrorAndAfter()
    {
        var statuses = new List<Status>();
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
            new List<ExecuteQueryResponsePart> { successPart, failPart, nextFailPart }), statuses.Add);

        Assert.True(reader.Read());
        Assert.Equal(@"Status: Aborted, Issues:
[0] Fatal: Some message 1
[0] Fatal: Some message 2
[0] Fatal: Some message 2
[0] Fatal: Some message 3
", Assert.Throws<YdbException>(() => reader.Read()).Message);
        Assert.Single(statuses);
        Assert.Equal(StatusCode.Aborted, statuses[0].StatusCode);
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
