using System.Text.Json;
using Xunit;
using Ydb.Query;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.TableStats;

namespace Ydb.Sdk.Ado.Tests;

public class YdbCommandStatsTests : TestBase
{
    [Theory]
    [InlineData(0, 0, StatsMode.Full)]
    [InlineData(1, 0, StatsMode.None)]
    [InlineData(1, 0, StatsMode.Basic)]
    [InlineData(1, 0, StatsMode.Full)]
    [InlineData(1, 0, StatsMode.Profile)]
    [InlineData(1, 0, StatsMode.Unspecified)]
    [InlineData(1, 1, StatsMode.Basic)]
    [InlineData(1, 2, StatsMode.Basic)]
    [InlineData(2, 0, StatsMode.Full)]
    public async Task QueryStats_AfterSuccessfulExecution_ContainsRequestedStatistics(
        int executionMethod, int sessionMode, StatsMode statsMode)
    {
        await using var connection = new YdbConnection(new YdbConnectionStringBuilder(ConnectionString)
        {
            EnableImplicitSession = sessionMode == 1
        });
        if (sessionMode == 2)
        {
            await connection.OpenAsync(new YdbRetryPolicyExecutor(new YdbRetryPolicy(new YdbRetryPolicyConfig())));
        }
        else
        {
            await connection.OpenAsync();
        }

        await using var command = new YdbCommand("SELECT 1; SELECT 2;", connection) { StatsMode = statsMode };
        Assert.Null(command.QueryStats);
        switch (executionMethod)
        {
            case 0:
                await using (var reader = await command.ExecuteReaderAsync())
                {
                    var values = new List<int>();
                    do
                    {
                        while (await reader.ReadAsync())
                        {
                            values.Add(reader.GetInt32(0));
                        }
                    } while (await reader.NextResultAsync());

                    Assert.Equal([1, 2], values);
                }

                break;
            case 1:
                await command.ExecuteNonQueryAsync();
                break;
            case 2:
                Assert.Equal(1, await command.ExecuteScalarAsync());
                break;
        }

        if (statsMode == StatsMode.None)
        {
            Assert.Null(command.QueryStats);
        }
        else if (statsMode != StatsMode.Unspecified)
        {
            var stats = Assert.IsType<YdbQueryStats>(command.QueryStats);
            Assert.True(stats.TotalDurationUs > 0);
            if (statsMode is StatsMode.Full or StatsMode.Profile)
            {
                using var plan = JsonDocument.Parse(stats.QueryPlan);
                Assert.Equal(JsonValueKind.Object, plan.RootElement.ValueKind);
            }
        }
    }

    [Fact]
    public async Task QueryStats_NewExecutionClearsPreviousStatistics()
    {
        await using var connection = await CreateOpenConnectionAsync();
        await using var command = new YdbCommand("SELECT 1;", connection) { StatsMode = StatsMode.Full };
        await command.ExecuteNonQueryAsync();
        Assert.NotNull(command.QueryStats);

        command.StatsMode = StatsMode.None;
        await command.ExecuteScalarAsync();
        Assert.Null(command.QueryStats);

        command.StatsMode = StatsMode.Full;
        await command.ExecuteNonQueryAsync();
        Assert.NotNull(command.QueryStats);
        using var cancellation = new CancellationTokenSource();
        cancellation.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => command.ExecuteScalarAsync(cancellation.Token));
        Assert.Null(command.QueryStats);

        await command.ExecuteNonQueryAsync();
        Assert.NotNull(command.QueryStats);
        var previousStats = command.QueryStats;
        command.CommandText = "SELECT FROM";
        await Assert.ThrowsAsync<YdbException>(() => command.ExecuteNonQueryAsync());
        Assert.NotSame(previousStats, command.QueryStats);
    }

    [Fact]
    public async Task QueryStats_ReaderCloseReceivesStatisticsFromResponsePart()
    {
        await using var connection = await CreateOpenConnectionAsync();
        await using var command = new YdbCommand(connection);
        var responses = new[]
        {
            new ExecuteQueryResponsePart
            {
                Status = StatusIds.Types.StatusCode.Success,
                ResultSet = new ResultSet()
            },
            new ExecuteQueryResponsePart
            {
                Status = StatusIds.Types.StatusCode.Success,
                ExecStats = new QueryStats { TotalDurationUs = 23 }
            }
        };
        await using var reader = await YdbDataReader.CreateYdbDataReader(
            new MockAsyncEnumerator<ExecuteQueryResponsePart>(responses), command, null, 0);
        await reader.CloseAsync();

        Assert.Equal((ulong)23, Assert.IsType<YdbQueryStats>(command.QueryStats).TotalDurationUs);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task QueryStats_ResponsePartsAssignStatisticsImmediately(bool fails)
    {
        await using var connection = await CreateOpenConnectionAsync();
        await using var command = new YdbCommand(connection);
        var responses = new[]
        {
            new ExecuteQueryResponsePart
            {
                Status = StatusIds.Types.StatusCode.Success,
                ResultSet = new ResultSet(),
                ExecStats = new QueryStats { TotalDurationUs = 1 }
            },
            new ExecuteQueryResponsePart
            {
                Status = StatusIds.Types.StatusCode.Success,
                ExecStats = new QueryStats { TotalDurationUs = 23 }
            },
            new ExecuteQueryResponsePart
            {
                Status = fails ? StatusIds.Types.StatusCode.Aborted : StatusIds.Types.StatusCode.Success
            }
        };
        await using var reader = await YdbDataReader.CreateYdbDataReader(
            new MockAsyncEnumerator<ExecuteQueryResponsePart>(responses), command, null, 0);
        Assert.Equal((ulong)1, Assert.IsType<YdbQueryStats>(command.QueryStats).TotalDurationUs);

        if (fails)
        {
            await Assert.ThrowsAsync<YdbException>(() => reader.NextResultAsync());
        }
        else
        {
            Assert.False(await reader.NextResultAsync());
        }

        Assert.Equal((ulong)23, Assert.IsType<YdbQueryStats>(command.QueryStats).TotalDurationUs);
    }

    [Fact]
    public async Task QueryStats_EmptyStreamDoesNotPublishStatistics()
    {
        await using var connection = await CreateOpenConnectionAsync();
        await using var command = new YdbCommand(connection);

        await Assert.ThrowsAsync<YdbException>(() => YdbDataReader.CreateYdbDataReader(
            new MockAsyncEnumerator<ExecuteQueryResponsePart>([]), command, null, 0));
        Assert.Null(command.QueryStats);
    }
}
