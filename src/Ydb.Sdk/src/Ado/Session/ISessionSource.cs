namespace Ydb.Sdk.Ado.Session;

internal interface ISessionSource<TSession> where TSession : ISession
{
    ValueTask<TSession> OpenSession(CancellationToken cancellationToken);

    void Return(TSession session);
}
