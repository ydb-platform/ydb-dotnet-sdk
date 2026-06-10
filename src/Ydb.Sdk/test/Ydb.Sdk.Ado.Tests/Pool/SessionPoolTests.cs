using Google.Protobuf.Collections;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Issue;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests.Pool;

public class SessionPoolTests
{
    internal const int TestSessionPoolSize = 50;

    private readonly TestSessionPool _testSessionPool = new();

    [Fact]
    public void GetSession_WhenRaceConditionThenRelease_CreateSuccessSessionIsCalledWithSizeLimit() =>
        StressTestSessionPoolAndCheckCreatedSessions(10_000, TestSessionPoolSize);

    [Fact]
    public async Task GetSession_WhenCreateSessionReturnUnavailable_ExpectedStatusAndReleaseSessionInPool()
    {
        _testSessionPool.ThrowException =
            YdbException.FromServer(StatusIds.Types.StatusCode.Unavailable, new RepeatedField<IssueMessage>());

        var e = await Assert.ThrowsAsync<YdbException>(async () => await _testSessionPool.GetSession());

        _testSessionPool.ThrowException = null;

        Assert.Equal(StatusCode.Unavailable, e.Code);

        StressTestSessionPoolAndCheckCreatedSessions(100, TestSessionPoolSize + 1);
    }

    [Theory]
    [InlineData(StatusCode.Unavailable)]
    [InlineData(StatusCode.BadSession)]
    [InlineData(StatusCode.SessionBusy)]
    [InlineData(StatusCode.InternalError)]
    public void GetSession_WhenCreatedSessionIsInvalidated_ExpectedRecreatedSession(StatusCode statusCode)
    {
        StressTestSessionPoolAndCheckCreatedSessions(100, TestSessionPoolSize);
        StressTestSessionPoolAndCheckCreatedSessions(70, 70, session => session.OnNotSuccessStatusCode(statusCode));
        StressTestSessionPoolAndCheckCreatedSessions(100, 120);
    }

    private void StressTestSessionPoolAndCheckCreatedSessions(int parallelTasks, int expectedCreatedSessions,
        Action<TestSession>? onSession = null)
    {
        onSession ??= _ => { };

        var tasks = new Task[parallelTasks];

        for (var i = 0; i < parallelTasks; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var session = await _testSessionPool.GetSession();

                await Task.Delay(100);

                onSession.Invoke(session);

                await session.Release();
            });
        }

        Task.WaitAll(tasks);

        Assert.True(_testSessionPool.InvokedCreateSession <= expectedCreatedSessions);
    }
}

internal class TestSessionPool() : SessionPool<TestSession>(NullLogger<TestSessionPool>.Instance,
    new SessionPoolConfig(MaxSessionPool: SessionPoolTests.TestSessionPoolSize, CreateSessionTimeout: 0))
{
    public volatile int InvokedCreateSession;

    public Exception? ThrowException { get; set; }

    protected override Task<TestSession> CreateSession(CancellationToken cancellationToken = default)
    {
        Interlocked.Increment(ref InvokedCreateSession);

        return Task.FromResult(((Func<TestSession>)(() =>
            ThrowException != null ? throw ThrowException : new TestSession(this)))());
    }
}

public class TestSession : SessionBase<TestSession>
{
    internal TestSession(SessionPool<TestSession> sessionPool)
        : base(sessionPool, "0", 0, TestUtils.LoggerFactory.CreateLogger<TestSession>())
    {
    }

    internal override Task DeleteSession() => Task.CompletedTask;
}
