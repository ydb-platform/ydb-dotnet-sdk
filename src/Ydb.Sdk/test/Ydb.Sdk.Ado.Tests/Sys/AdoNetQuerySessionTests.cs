using Xunit;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Ado.Tests.Sys;

public class AdoNetQuerySessionTests : TestBase
{
    [Fact]
    public async Task QuerySession_ReportsPid()
    {
        var expectedPid = Environment.ProcessId.ToString();

        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        dbCommand.CommandText = "SELECT ClientPID FROM `.sys/query_sessions` WHERE ClientPID = @pid;";
        dbCommand.Parameters.Add(new YdbParameter("pid", expectedPid));

        await dbCommand.ExecuteNonQueryAsync();
        var actualPid = await dbCommand.ExecuteScalarAsync();

        Assert.Equal(expectedPid, actualPid);
    }

    [Fact]
    public async Task QuerySession_ReportsSdkBuildInfoWithAdoNetClientInfo()
    {
        var expected = $"ydb-dotnet-sdk/{YdbSdkVersion.Value};{Metadata.AdoNetClientInfo}";

        await using var connection = await CreateOpenConnectionAsync();
        var dbCommand = connection.CreateCommand();
        // The session running this query was created by the ADO.NET provider, so its ClientSdkBuildInfo
        // must carry both the base SDK token and the ado-net component reported on every call.
        dbCommand.CommandText =
            "SELECT ClientSdkBuildInfo FROM `.sys/query_sessions` WHERE ClientPID = @pid AND ClientSdkBuildInfo = @info;";
        dbCommand.Parameters.Add(new YdbParameter("info", expected));

        var actual = await dbCommand.ExecuteScalarAsync();

        Assert.Equal(expected, actual);
    }
}
