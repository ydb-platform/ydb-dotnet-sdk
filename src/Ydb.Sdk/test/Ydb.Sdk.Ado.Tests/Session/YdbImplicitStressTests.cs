using Moq;
using Xunit;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado.Tests.Session;

public class YdbImplicitStressTests : TestBase
{
    private static IDriver DummyDriver()
    {
        var m = new Mock<IDriver>(MockBehavior.Loose);
        m.Setup(d => d.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return m.Object;
    }

    private sealed class Counter
    {
        public int Value;
        public void Inc() => Interlocked.Increment(ref Value);
    }

    [Fact(Timeout = 30_000)]
    public async Task Dispose_WaitsForAllLeases_AndSignalsOnEmptyExactlyOnce()
    {
        var driver = DummyDriver();

        var opened = new Counter();
        var closed = new Counter();

        var source = new ImplicitSessionSource(driver);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var workers = Enumerable.Range(0, 200).Select(async _ =>
        {
            var rnd = Random.Shared;
            for (var j = 0; j < 10; j++)
            {
                try
                {
                    var s = await source.OpenSession(cts.Token);
                    opened.Inc();

                    await Task.Delay(rnd.Next(0, 5), cts.Token);

                    s.Close();
                    closed.Inc();
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

        Assert.True(opened.Value > 0);
        Assert.Equal(opened.Value, closed.Value);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => source.OpenSession(CancellationToken.None).AsTask());
    }

    [Fact(Timeout = 30_000)]
    public async Task Stress_Counts_AreBalanced()
    {
        var driver = DummyDriver();

        var opened = new Counter();
        var closed = new Counter();

        var source = new ImplicitSessionSource(driver);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));

        var workers = Enumerable.Range(0, 200).Select(async _ =>
        {
            var rnd = Random.Shared;
            for (var j = 0; j < 10; j++)
            {
                try
                {
                    var s = await source.OpenSession(cts.Token);
                    opened.Inc();

                    await Task.Delay(rnd.Next(0, 3), cts.Token);

                    s.Close();
                    closed.Inc();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        }).ToArray();

        var disposer = Task.Run(async () => await source.DisposeAsync(), cts.Token);

        await Task.WhenAll(workers.Append(disposer));

        Assert.Equal(opened.Value, closed.Value);
        Assert.True(opened.Value > 0);

        await Assert.ThrowsAsync<ObjectDisposedException>(() => source.OpenSession(CancellationToken.None).AsTask());
    }

    [Fact(Timeout = 30_000)]
    public async Task Open_RacingWithDispose_StateRemainsConsistent()
    {
        var driver = DummyDriver();

        var source = new ImplicitSessionSource(driver);

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

        await Assert.ThrowsAsync<ObjectDisposedException>(() => source.OpenSession(CancellationToken.None).AsTask());
    }
}
