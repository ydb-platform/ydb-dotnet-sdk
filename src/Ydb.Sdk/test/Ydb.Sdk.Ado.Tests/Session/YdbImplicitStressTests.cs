using Moq;
using Xunit;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests.Session;

public class YdbImplicitStressTests
{
    private volatile bool _isDisposed;

    private IDriver DummyDriver()
    {
        var m = new Mock<IDriver>(MockBehavior.Loose);
        m.Setup(d => d.DisposeAsync())
            .Callback(() => _isDisposed = true)
            .Returns(ValueTask.CompletedTask);
        return m.Object;
    }

    [Fact]
    public async Task StressTest_OpenSession_RaceWithDispose_SuccessfulOpensAreNotDisposed()
    {
        for (var it = 0; it < 1000; it++)
        {
            var driver = DummyDriver();
            var source = new ImplicitSessionSource(driver, TestUtils.LoggerFactory);

            var workers = Enumerable.Range(0, 1000).Select(async _ =>
            {
                await Task.Delay(Random.Shared.Next(0, 5));
                try
                {
                    using var s = await source.OpenSession(CancellationToken.None);
                    Assert.False(_isDisposed);
                }
                catch (ObjectDisposedException)
                {
                }
            }).ToArray();

            await Task.WhenAll(workers.Append(Task.Run(async () =>
            {
                await Task.Delay(Random.Shared.Next(0, 3));
                await source.DisposeAsync();
            })));

            Assert.True(_isDisposed);
            await Assert.ThrowsAsync<ObjectDisposedException>(() =>
                source.OpenSession(CancellationToken.None).AsTask());
            _isDisposed = false;
        }
    }

    [Fact]
    public async Task DisposeAsync_WhenSessionIsLeaked_ThrowsYdbExceptionWithTimeoutMessage()
    {
        var driver = DummyDriver();
        var source = new ImplicitSessionSource(driver, TestUtils.LoggerFactory);
#pragma warning disable CA2012
        _ = source.OpenSession(CancellationToken.None);
#pragma warning restore CA2012

        Assert.Equal("Timeout while disposing of the pool: some implicit sessions are still active. " +
                     "This may indicate a connection leak or suspended operations.",
            (await Assert.ThrowsAsync<YdbException>(async () => await source.DisposeAsync())).Message);
        Assert.True(_isDisposed);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => source.OpenSession(CancellationToken.None).AsTask());
    }
}
