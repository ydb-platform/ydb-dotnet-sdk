using Xunit;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
public class YdbConnectionTests
{
    private volatile int _counter;

    [Fact]
    public async Task ClearPool_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearPool(new YdbConnection("MaxSessionPool=10")));

        tasks.AddRange(GenerateTasks());

        await Task.WhenAll(tasks);
        Assert.Equal(999000, _counter);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearPool(new YdbConnection("MaxSessionPool=10")));

        await Task.WhenAll(tasks);
        Assert.Equal(1498500, _counter);
    }

    [Fact]
    public async Task ClearPoolAllPools_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        tasks.AddRange(GenerateTasks());

        await Task.WhenAll(tasks);
        Assert.Equal(999000, _counter);

        tasks = GenerateTasks();

        tasks.Add(YdbConnection.ClearAllPools());

        await Task.WhenAll(tasks);
        Assert.Equal(1498500, _counter);
    }

    private List<Task> GenerateTasks()
    {
        return Enumerable.Range(0, 1000).Select(async i =>
        {
            await using var connection = new YdbConnection("MaxSessionPool=10");
            await connection.OpenAsync();

            var command = connection.CreateCommand();
            command.CommandText = "SELECT " + i;

            var scalar = (int)(await command.ExecuteScalarAsync())!;
            Assert.Equal(i, scalar);

            Interlocked.Add(ref _counter, scalar);
        }).ToList();
    }
}
