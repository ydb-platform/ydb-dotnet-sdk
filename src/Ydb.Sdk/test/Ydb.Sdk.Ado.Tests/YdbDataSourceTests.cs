using Xunit;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Tests.Session;

namespace Ydb.Sdk.Ado.Tests;

public class YdbDataSourceTests : TestBase
{
    private const int SelectedCount = 100;

    private readonly YdbDataSource _dataSource = new(ConnectionString);

    [Fact]
    public async Task OpenConnectionAsync_WhenMaxSessionPool10_ReturnOpenConnection()
    {
        var tasks = new Task[SelectedCount];
        for (var i = 0; i < SelectedCount; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                await using var ydbConnection = await _dataSource.OpenConnectionAsync();
                var ydbCommand = new YdbCommand(ydbConnection) { CommandText = "SELECT 1" };
                Assert.Equal(1, await ydbCommand.ExecuteScalarAsync());
            });
        }

        await Task.WhenAll(tasks);
    }

    [Fact]
    public void CreateCommand_FromDataSource_ReturnDbCommand()
    {
        var dataSource = new YdbDataSource(ConnectionString + ";MaxSessionPool=5");
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }

        dataSource.Dispose();
        for (var i = 0; i < SelectedCount; i++)
        {
            using var command = dataSource.CreateCommand("SELECT 1;");
            Assert.Equal(1, command.ExecuteScalar());
        }

        dataSource.Dispose();
    }

    [Fact]
    public void CreateConnection_FromDataSource_ReturnNotOpenConnection()
    {
        using var ydbConnection = _dataSource.CreateConnection();
        ydbConnection.Open();
        Assert.Equal(1, new YdbCommand(ydbConnection) { CommandText = "SELECT 1" }.ExecuteScalar());
    }

    [Fact]
    public void OpenConnection_FromDataSource_ReturnOpenConnection()
    {
        using var ydbConnection = _dataSource.OpenConnection();
        Assert.Equal(1, new YdbCommand(ydbConnection) { CommandText = "SELECT 1" }.ExecuteScalar());
    }

    [Fact]
    public async Task ExecuteAsync_WhenBadSession_ThenRetriesUntilSuccess()
    {
        var attempt = 0;
        await _dataSource.ExecuteAsync(_ =>
        {
            if (attempt++ < 3)
            {
                throw new YdbException(StatusCode.BadSession, "Bad Session");
            }

            return Task.CompletedTask;
        });
    }

    [Theory]
    [InlineData(StatusCode.Undetermined)]
    [InlineData(StatusCode.ClientTransportUnknown)]
    [InlineData(StatusCode.ClientTransportUnavailable)]
    public async Task ExecuteAsync_WhenIsIdempotenceConfig_ThenRetriesUntilSuccess(StatusCode statusCode)
    {
        var attempt = 0;
        await _dataSource.ExecuteAsync(_ =>
        {
            if (attempt++ < 3)
            {
                throw new YdbException(statusCode, "Bad Session");
            }

            return Task.CompletedTask;
        });
    }

    [Theory]
    [InlineData(StatusCode.BadRequest)]
    [InlineData(StatusCode.SchemeError)]
    [InlineData(StatusCode.NotFound)]
    public async Task ExecuteAsync_WhenNonRetryableStatus_ThenThrowsWithoutRetry(StatusCode code)
    {
        var attempt = 0;

        var ex = await Assert.ThrowsAsync<YdbException>(() =>
            _dataSource.ExecuteAsync(_ =>
            {
                attempt++;
                throw new YdbException(code, "Non-retryable");
            }));

        Assert.Equal(code, ex.Code);
        Assert.Equal(1, attempt);
    }

    [Fact]
    public async Task ExecuteAsync_WhenAlwaysRetryableAndMaxAttemptsReached_ThenThrowsLastException()
    {
        var attempt = 0;
        var config = new YdbRetryPolicyConfig { MaxAttempts = 3 }; // как у вас конфигурируется

        var ex = await Assert.ThrowsAsync<YdbException>(() => _dataSource.ExecuteAsync(_ =>
        {
            attempt++;
            throw new YdbException(StatusCode.BadSession, "Still bad");
        }, config));

        Assert.Equal(3, attempt);
        Assert.Equal(StatusCode.BadSession, ex.Code);
    }

    [Fact]
    public async Task ExecuteAsync_WhenTokenPreCanceled_ThenDoesNotInvokeDelegate()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var called = false;
        await Assert.ThrowsAsync<OperationCanceledException>(() =>
            _dataSource.ExecuteAsync((_, _) =>
            {
                called = true;
                return Task.CompletedTask;
            }, cts.Token));

        Assert.False(called);
    }

    [Fact]
    public async Task ExecuteAsync_WhenUserCodeThrows_ThenDoesNotRetry()
    {
        var attempt = 0;
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            _dataSource.ExecuteAsync(_ =>
            {
                attempt++;
                throw new InvalidOperationException("Bug");
            }));

        Assert.Equal(1, attempt);
    }

    [Fact]
    public async Task ExecuteAsync_WhenBadSession_ThenCreatesNewSessionPerAttempt()
    {
        var ydbConnections = new List<YdbConnection>();

        var attempt = 0;
        await _dataSource.ExecuteAsync(ydbConnection =>
        {
            ydbConnection.OnNotSuccessStatusCode(StatusCode.BadSession);
            ydbConnections.Add(ydbConnection);
            if (attempt++ < 2)
                throw new YdbException(StatusCode.BadSession, "Bad");
            return Task.CompletedTask;
        });

        Assert.Equal(3, attempt);
        Assert.Equal(3, ydbConnections.Count);
        Assert.True(ydbConnections.Distinct().Count() == ydbConnections.Count); // new one every time
    }
}
