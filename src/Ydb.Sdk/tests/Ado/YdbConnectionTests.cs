using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
public class YdbConnectionTests
{
    [Fact]
    public async Task ClearPool_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearPool(new YdbConnection()));

        await Task.WhenAll(tasks);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearPool(new YdbConnection()));

        await Task.WhenAll(tasks);
    }

    [Fact]
    public async Task ClearPoolAllPools_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        await using var connection = new YdbConnection();
        await connection.OpenAsync();

        var tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        await Task.WhenAll(tasks);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        await Task.WhenAll(tasks);
    }

    private static List<Task> GenerateTasks()
    {
        return Enumerable.Range(0, 10).Select(async i =>
        {
            await using var connection = new YdbConnection("MaxSessionPool=10");
            await connection.OpenAsync();

            var command = connection.CreateCommand();
            command.CommandText = "SELECT " + i;

            Assert.Equal(i, await command.ExecuteScalarAsync());
        }).ToList();
    }
}
