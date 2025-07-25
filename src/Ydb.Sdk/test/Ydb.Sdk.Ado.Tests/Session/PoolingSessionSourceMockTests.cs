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
        var sessionSource =
            new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(),
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
    }
}

internal static class ISessionExtension
{
    internal static string SessionId(this ISession session) => ((MockPoolingSession)session).SessionId;
}

internal class MockPoolingSessionFactory : IPoolingSessionFactory<MockPoolingSession>
{
    private int _sessionNum;

    public MockPoolingSession NewSession(PoolingSessionSource<MockPoolingSession> source) =>
        new(source, Interlocked.Increment(ref _sessionNum));
}

internal class MockPoolingSession(PoolingSessionSource<MockPoolingSession> source, int sessionNum)
    : PoolingSessionBase<MockPoolingSession>(source)
{
    public string SessionId => $"session_{sessionNum}";
    public override IDriver Driver => null!;
    public override bool IsBroken => false;

    internal override Task Open(CancellationToken cancellationToken) => Task.CompletedTask;
    internal override Task DeleteSession() => Task.CompletedTask;

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
