using Xunit;
using Ydb.Query;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Tests.Session;

public class PoolingSessionSourceMockTests
{
    [Fact]
    public void MinSessionPool_bigger_than_MaxSessionPool_throws() => Assert.Throws<ArgumentException>(() =>
        new PoolingSessionSource(new MockPoolingSessionFactory(),
            new YdbConnectionStringBuilder { MaxSessionPool = 1, MinSessionPool = 2 })
    );

    [Fact]
    public async Task Reuse_Session_Before_Creating_new()
    {
        var sessionSource = new PoolingSessionSource(new MockPoolingSessionFactory(), new YdbConnectionStringBuilder());
        var session = (MockPoolingSession)await sessionSource.OpenSession();
        var sessionId = session.SessionId;
        session.Close();
        session = (MockPoolingSession)await sessionSource.OpenSession();
        Assert.Equal(sessionId, session.SessionId);
    }
}

internal class MockPoolingSessionFactory : IPoolingSessionFactory
{
    private int _sessionNum;

    public PoolingSession NewSession(PoolingSessionSource source) =>
        new MockPoolingSession(source, Interlocked.Increment(ref _sessionNum));
}

internal class MockPoolingSession(PoolingSessionSource source, int sessionNum) : PoolingSession(null, null, true, null)
{
    internal string SessionId { get; } = $"session_{sessionNum}";

    public IDriver Driver => throw new NotImplementedException();
    public bool IsBroken { get; set; }

    public void Close() => source.Return(this);

    public Task Open(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task DeleteSession() => Task.CompletedTask;

    public Task CommitTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public Task RollbackTransaction(string txId, CancellationToken cancellationToken = default) =>
        throw new NotImplementedException();

    public ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(string query,
        Dictionary<string, YdbValue> parameters, GrpcRequestSettings settings,
        TransactionControl? txControl) =>
        throw new NotImplementedException();

    public void OnNotSuccessStatusCode(StatusCode code) => throw new NotImplementedException();
}
