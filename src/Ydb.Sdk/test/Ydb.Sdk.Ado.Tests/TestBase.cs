using Xunit;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests;

public abstract class TestBase : IAsyncLifetime
{
    protected static string ConnectionString => TestUtils.ConnectionString;

    protected static YdbConnection CreateConnection() => new(
        new YdbConnectionStringBuilder(ConnectionString) { LoggerFactory = TestUtils.LoggerFactory }
    );

    protected static YdbConnection CreateOpenConnection()
    {
        var connection = CreateConnection();
        connection.Open();
        return connection;
    }

    protected static async Task<YdbConnection> CreateOpenConnectionAsync()
    {
        var connection = CreateConnection();
        await connection.OpenAsync();
        return connection;
    }

    public async Task InitializeAsync() => await OnInitializeAsync().ConfigureAwait(false);

    public async Task DisposeAsync() => await OnDisposeAsync().ConfigureAwait(false);

    protected virtual Task OnInitializeAsync() => Task.CompletedTask;

    protected virtual Task OnDisposeAsync() => Task.CompletedTask;
}
