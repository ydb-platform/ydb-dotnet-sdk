using Moq;
using Xunit;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado.Tests.Session;

public class YdbImplicitStressTests : TestBase
{
    private static IDriver DummyDriver() => new Mock<IDriver>(MockBehavior.Strict).Object;

    [Fact(Timeout = 30_000)]
    public async Task Dispose_WaitsForAllLeases_AndSignalsOnEmptyExactlyOnce()
    {
        var driver = DummyDriver();

        var onEmptyCalls = 0;
        var opened = 0;
        var closed = 0;

        var source = new ImplicitSessionSource(driver, onEmpty: () => Interlocked.Increment(ref onEmptyCalls));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var workers = Enumerable.Range(0, 200).Select(async i =>
        {
            var rnd = new Random(unchecked(i ^ Environment.TickCount));
            for (var j = 0; j < 10; j++)
            {
                try
                {
                    var s = await source.OpenSession(cts.Token);
                    Interlocked.Increment(ref opened);

                    await Task.Delay(rnd.Next(0, 5), cts.Token);

                    s.Close();
                    Interlocked.Increment(ref closed);
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }).ToArray();

        var disposer = Task.Run(async () =>
        {
            await Task.Delay(10, cts.Token);
            await source.DisposeAsync();
        }, cts.Token);

        await Task.WhenAll(workers.Append(disposer));

        Assert.True(opened > 0);
        Assert.Equal(opened, closed);
        Assert.Equal(1, Volatile.Read(ref onEmptyCalls));

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => source.OpenSession(CancellationToken.None).AsTask());
    }
    
    [Fact(Timeout = 30_000)]
    public async Task Stress_Counts_AreBalanced()
    {
        var driver = DummyDriver();

        var opened = 0;
        var closed = 0;
        var onEmptyCalls = 0;

        var source = new ImplicitSessionSource(driver, onEmpty: () => Interlocked.Increment(ref onEmptyCalls));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var workers = Enumerable.Range(0, 200).Select(async i =>
        {
            var rnd = new Random(unchecked(i ^ Environment.TickCount));
            for (var j = 0; j < 10; j++)
            {
                try
                {
                    var s = await source.OpenSession(cts.Token);
                    Interlocked.Increment(ref opened);

                    await Task.Delay(rnd.Next(0, 3), cts.Token);

                    s.Close();
                    Interlocked.Increment(ref closed);
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }).ToArray();

        var disposer = Task.Run(async () => await source.DisposeAsync(), cts.Token);

        await Task.WhenAll(workers.Append(disposer));

        Assert.Equal(opened, closed);
        Assert.Equal(1, onEmptyCalls);
        Assert.True(opened > 0);

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => source.OpenSession(CancellationToken.None).AsTask());
    }

    [Fact(Timeout = 30_000)]
    public async Task Open_RacingWithDispose_StateRemainsConsistent()
    {
        var driver = DummyDriver();

        var onEmptyCalls = 0;
        var source = new ImplicitSessionSource(driver, onEmpty: () => Interlocked.Increment(ref onEmptyCalls));

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var opens = Enumerable.Range(0, 1000).Select(async _ =>
        {
            try
            {
                var s = await source.OpenSession(cts.Token);
                s.Close();
                return 1;
            }
            catch (ObjectDisposedException)
            {
                return 0;
            }
        }).ToArray();

        var disposeTask = Task.Run(async () =>
        {
            await Task.Yield();
            await source.DisposeAsync();
        }, cts.Token);

        await Task.WhenAll(opens.Append(disposeTask));

        Assert.Equal(1, Volatile.Read(ref onEmptyCalls));

        await Assert.ThrowsAsync<ObjectDisposedException>(
            () => source.OpenSession(CancellationToken.None).AsTask());
    }
}
