namespace Ydb.Sdk.Ado.Session;

internal interface ISessionSource : IAsyncDisposable
{
    ValueTask<ISession> OpenSession(CancellationToken cancellationToken);
}
