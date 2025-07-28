using Xunit;
using Ydb.Query;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests.Session;

public class PoolingSessionSourceMockTests
{
    [Fact]
    public void MinSessionPool_bigger_than_MaxSessionPool_throws() => Assert.Throws<ArgumentException>(() =>
        new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(),
            new YdbConnectionStringBuilder { MaxSessionPool = 1, MinSessionPool = 2 })
    );

    [Fact]
    public async Task Reuse_Session_Before_Creating_new()
    {
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(),
            new YdbConnectionStringBuilder());
        var session = await sessionSource.OpenSession();
        var sessionId = session.SessionId();
        session.Close();
        session = await sessionSource.OpenSession();
        Assert.Equal(sessionId, session.SessionId());
    }

    [Fact]
    public async Task Creating_Session_Throw_Exception()
    {
        for (var it = 0; it < 10_000; it++)
        {
            const string errorMessage = "Error on open session";
            const int maxSessionSize = 200;

            var mockPoolingSessionFactory = new MockPoolingSessionFactory
            {
                Open = sessionNum =>
                    sessionNum <= maxSessionSize * 2
                        ? Task.FromException(new YdbException(errorMessage))
                        : Task.CompletedTask
            };

            var sessionSource = new PoolingSessionSource<MockPoolingSession>(
                mockPoolingSessionFactory, new YdbConnectionStringBuilder { MaxSessionPool = maxSessionSize }
            );

            var tasks = new List<Task>();
            var countSuccess = 0;

            for (var i = 0; i < maxSessionSize * 4; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        var session = await sessionSource.OpenSession();
                        // ReSharper disable once AccessToModifiedClosure
                        Interlocked.Increment(ref countSuccess);
                        Assert.True(session.SessionId() > maxSessionSize * 2);
                        session.Close();
                    }
                    catch (YdbException e)
                    {
                        Assert.Equal(errorMessage, e.Message);
                    }
                }));
            }

            await Task.WhenAll(tasks);
            Assert.Equal(maxSessionSize * 2, Volatile.Read(ref countSuccess));
            Assert.True(maxSessionSize * 3 >= mockPoolingSessionFactory.SessionNum);
            Assert.True(maxSessionSize * 2 < mockPoolingSessionFactory.SessionNum);
        }
    }

    [Fact]
    public async Task HighContention_OpenClose_NotCanceledException()
    {
        var mockPoolingSessionFactory = new MockPoolingSessionFactory
        {
            Open = async _ => await Task.Yield()
        };
        const int highContentionTasks = 100;
        const int maxSessionSize = highContentionTasks / 2;

        var sessionSource = new PoolingSessionSource<MockPoolingSession>(
            mockPoolingSessionFactory, new YdbConnectionStringBuilder { MaxSessionPool = maxSessionSize }
        );

        for (var it = 0; it < 100_000; it++)
        {
            var tasks = new Task[highContentionTasks];

            for (var i = 0; i < highContentionTasks; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var session = await sessionSource.OpenSession();
                    Assert.True(session.SessionId() <= maxSessionSize);
                    await Task.Yield();
                    session.Close();
                });
            }

            await Task.WhenAll(tasks);
        }
    }
}

internal static class ISessionExtension
{
    internal static int SessionId(this ISession session) => ((MockPoolingSession)session).SessionId;
}

internal class MockPoolingSessionFactory : IPoolingSessionFactory<MockPoolingSession>
{
    private int _sessionNum;

    internal int SessionNum => Volatile.Read(ref _sessionNum);

    internal Func<int, Task> Open { private get; init; } = _ => Task.CompletedTask;

    internal Func<int, Task> DeleteSession { private get; init; } = _ => Task.CompletedTask;

    internal Func<bool> IsBroken { private get; init; } = () => false;

    public MockPoolingSession NewSession(PoolingSessionSource<MockPoolingSession> source) =>
        new(source, Open, DeleteSession, IsBroken, Interlocked.Increment(ref _sessionNum));
}

internal class MockPoolingSession(
    PoolingSessionSource<MockPoolingSession> source,
    Func<int, Task> mockOpen,
    Func<int, Task> mockDeleteSession,
    Func<bool> mockIsBroken,
    int sessionNum
) : PoolingSessionBase<MockPoolingSession>(source)
{
    public int SessionId => sessionNum;
    public override IDriver Driver => null!;
    public override bool IsBroken => mockIsBroken();

    internal override Task Open(CancellationToken cancellationToken) => mockOpen(sessionNum);
    internal override Task DeleteSession() => mockDeleteSession(sessionNum);

    public override ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, YdbValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl
    ) => throw new NotImplementedException();

    public override Task CommitTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public override Task RollbackTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public override void OnNotSuccessStatusCode(StatusCode code)
    {
    }
}
