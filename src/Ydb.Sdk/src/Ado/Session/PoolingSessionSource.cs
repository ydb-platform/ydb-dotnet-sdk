namespace Ydb.Sdk.Ado.Session;

internal class PoolingSessionSource : ISessionSource<IPoolingSession>
{
    public ValueTask<IPoolingSession> OpenSession() => throw new NotImplementedException();

    public void Return(IPoolingSession session) => throw new NotImplementedException();
}

internal interface IPoolingSession : ISession
{
    bool IsActive { get; }

    Task Open(CancellationToken cancellationToken);

    Task DeleteSession();
}
