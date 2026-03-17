namespace Ydb.Sdk.Ado.Session;

internal interface ISessionSource : IAsyncDisposable
{
    IDriver Driver { get; }
    ValueTask<ISession> OpenSession(CancellationToken cancellationToken);
}
