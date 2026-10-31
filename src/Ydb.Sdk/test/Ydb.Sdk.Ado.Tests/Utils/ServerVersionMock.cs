using Moq;
using Ydb.Query;
using Ydb.Query.V1;

namespace Ydb.Sdk.Ado.Tests.Utils;

/// <summary>
/// Answers the Version() query that a connection sends when it opens. Mock drivers that do not set up
/// this query fail on open, so every mock driver that opens a connection calls this first.
/// </summary>
internal static class ServerVersionMock
{
    private const string VersionQuery = "SELECT CAST(Version() AS Utf8);";

    internal static bool IsVersionQuery(ExecuteQueryRequest request) => request.QueryContent.Text == VersionQuery;

    internal static void SetupVersionQuery(Mock<IDriver> driver) =>
        driver.Setup(d => d.ServerStreamCall(
                QueryService.ExecuteQueryMethod,
                It.Is<ExecuteQueryRequest>(r => IsVersionQuery(r)),
                It.IsAny<GrpcRequestSettings>()))
            .Returns(new ValueTask<IServerStream<ExecuteQueryResponsePart>>(
                new MockAsyncEnumerator<ExecuteQueryResponsePart>([
                    new ExecuteQueryResponsePart
                    {
                        Status = StatusIds.Types.StatusCode.Success,
                        ResultSet = new ResultSet
                        {
                            Rows = { new Ydb.Value { Items = { new Ydb.Value { TextValue = "26.2.1.14" } } } }
                        }
                    }
                ])));
}
