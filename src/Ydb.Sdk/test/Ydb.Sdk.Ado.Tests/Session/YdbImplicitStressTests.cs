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

        var workers = Enumerable.Range(0, 200).Select(async _ =>
        {
            var rnd = Random.Shared;
            for (var j = 0; j < 10; j++)
            {
                ISession s;
                try
                {
                    s = await source.OpenSession(CancellationToken.None);
                    opened.Inc();

                    await Task.Delay(rnd.Next(0, 5));
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                var s2 = await source.OpenSession(CancellationToken.None);
                s2.Dispose();

                s.Dispose();
                closed.Inc();
            }
        }).ToArray();

        var disposer = Task.Run(async () =>
        {
            await Task.Delay(10);
            await source.DisposeAsync();
        });

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

        var workers = Enumerable.Range(0, 200).Select(async _ =>
        {
            var rnd = Random.Shared;
            for (var j = 0; j < 10; j++)
            {
                ISession s;
                try
                {
                    s = await source.OpenSession(CancellationToken.None);
                    opened.Inc();

                    await Task.Delay(rnd.Next(0, 3));
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                var s2 = await source.OpenSession(CancellationToken.None);
                s2.Dispose();

                s.Dispose();
                closed.Inc();
            }
        }).ToArray();

        var disposer = Task.Run(async () => await source.DisposeAsync());

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

        var opens = Enumerable.Range(0, 1000).Select(async _ =>
        {
            ISession s;
            try
            {
                s = await source.OpenSession(CancellationToken.None);
            }
            catch (ObjectDisposedException)
            {
                return 0;
            }

            var s2 = await source.OpenSession(CancellationToken.None);
            s2.Dispose();

            s.Dispose();
            return 1;
        }).ToArray();

        var disposeTask = Task.Run(async () =>
        {
            await Task.Yield();
            await source.DisposeAsync();
        });

        await Task.WhenAll(opens.Append(disposeTask));

        await Assert.ThrowsAsync<ObjectDisposedException>(() => source.OpenSession(CancellationToken.None).AsTask());
    }
}
