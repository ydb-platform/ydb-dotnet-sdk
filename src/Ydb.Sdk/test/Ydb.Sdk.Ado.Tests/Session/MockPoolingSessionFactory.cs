using Xunit;
using Ydb.Query;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado.Tests.Session;

internal class MockPoolingSessionFactory(int maxSessionSize) : IPoolingSessionFactory<MockPoolingSession>
{
    private int _sessionOpened;
    private int _numSession;

    internal int SessionOpenedCount => Volatile.Read(ref _sessionOpened);
    internal int NumSession => Volatile.Read(ref _numSession);

    internal Func<int, Task> Open { private get; init; } = _ => Task.CompletedTask;
    internal Func<int, bool> IsBroken { private get; init; } = _ => false;
    internal Func<ValueTask> Dispose { private get; init; } = () => ValueTask.CompletedTask;

    internal Func<int, IServerStream<ExecuteQueryResponsePart>> ExecuteQuery { private get; init; } =
        _ => throw new NotImplementedException();

    public MockPoolingSession NewSession(PoolingSessionSource<MockPoolingSession> source) =>
        new(source,
            async sessionCountOpened =>
            {
                await Open(sessionCountOpened);

                Assert.True(Interlocked.Increment(ref _numSession) <= maxSessionSize);

                await Task.Yield();
            },
            () =>
            {
                Assert.True(Interlocked.Decrement(ref _numSession) >= 0);

                return Task.CompletedTask;
            },
            IsBroken,
            ExecuteQuery,
            Interlocked.Increment(ref _sessionOpened)
        );

    public ValueTask DisposeAsync() => Dispose();
}

internal class MockPoolingSession(
    PoolingSessionSource<MockPoolingSession> source,
    Func<int, Task> mockOpen,
    Func<Task> mockDeleteSession,
    Func<int, bool> mockIsBroken,
    Func<int, IServerStream<ExecuteQueryResponsePart>> executeQuery,
    int sessionId
) : PoolingSessionBase<MockPoolingSession>(source)
{
    private bool _isBroken;

    public int SessionId => sessionId;
    public override IDriver Driver => null!;
    public override bool IsBroken => _isBroken || mockIsBroken(sessionId);

    internal override Task Open(CancellationToken cancellationToken) => mockOpen(sessionId);
    internal override Task DeleteSession() => mockDeleteSession();

    public override ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
        string query,
        Dictionary<string, TypedValue> parameters,
        GrpcRequestSettings settings,
        TransactionControl? txControl
    ) => new(executeQuery(sessionId));

    public override Task CommitTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public override Task RollbackTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public override void OnNotSuccessStatusCode(StatusCode code) => _isBroken = true;
}

internal static class ISessionExtension
{
    internal static int SessionId(this ISession session) => ((MockPoolingSession)session).SessionId;
}
