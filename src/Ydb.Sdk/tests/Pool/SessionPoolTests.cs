using Google.Protobuf.Collections;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Issue;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Tests.Pool;

[Trait("Category", "Unit")]
public class SessionPoolTests
{
    public const int TestSessionPoolSize = 50;

    private readonly TestSessionPool _testSessionPool = new();

    [Fact]
    public void GetSession_WhenRaceConditionThenRelease_CreateSuccessSessionIsCalledWithSizeLimit()
    {
        StressTestSessionPoolAndCheckCreatedSessions(10_000, TestSessionPoolSize);
    }

    [Fact]
    public async Task GetSession_WhenCreateSessionReturnUnavailable_ExpectedStatusAndReleaseSessionInPool()
    {
        _testSessionPool.CreatedStatus = Status
            .FromProto(StatusIds.Types.StatusCode.Unavailable, new RepeatedField<IssueMessage>());

        var (status, session) = await _testSessionPool.GetSession();

        Assert.Equal(StatusCode.Unavailable, status.StatusCode);
        Assert.Null(session);

        _testSessionPool.CreatedStatus = Status.Success;

        StressTestSessionPoolAndCheckCreatedSessions(100, TestSessionPoolSize + 1);
    }

    [Theory]
    [InlineData(StatusIds.Types.StatusCode.Unavailable)]
    [InlineData(StatusIds.Types.StatusCode.BadSession)]
    [InlineData(StatusIds.Types.StatusCode.SessionBusy)]
    [InlineData(StatusIds.Types.StatusCode.InternalError)]
    public void GetSession_WhenCreatedSessionIsInvalidated_ExpectedRecreatedSession(
        StatusIds.Types.StatusCode statusCode)
    {
        StressTestSessionPoolAndCheckCreatedSessions(100, TestSessionPoolSize);

        StressTestSessionPoolAndCheckCreatedSessions(70, 70,
            session => session.OnStatus(Status.FromProto(statusCode, new RepeatedField<IssueMessage>())));

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
                var (_, session) = await _testSessionPool.GetSession();

                await Task.Delay(100);

                onSession.Invoke(session!);

                session!.Release();
            });
        }

        Task.WaitAll(tasks);

        Assert.Equal(expectedCreatedSessions, _testSessionPool.InvokedCreateSession);
    }
}

internal class TestSessionPool : SessionPool<TestSession>
{
    public int InvokedCreateSession;

    public Status CreatedStatus { get; set; } = Status.Success;

    public TestSessionPool() : base(NullLogger<TestSessionPool>.Instance, SessionPoolTests.TestSessionPoolSize)
    {
    }

    protected override Task<(Status, TestSession?)> CreateSession()
    {
        Interlocked.Increment(ref InvokedCreateSession);

        return Task.FromResult((CreatedStatus, CreatedStatus.IsSuccess ? new TestSession(this) : null));
    }

    protected override Task<Status> DeleteSession()
    {
        return Task.FromResult(Status.Success);
    }
}

public class TestSession : SessionBase<TestSession>
{
    internal TestSession(SessionPool<TestSession> sessionPool) : base(sessionPool, "0", 0)
    {
    }
}
