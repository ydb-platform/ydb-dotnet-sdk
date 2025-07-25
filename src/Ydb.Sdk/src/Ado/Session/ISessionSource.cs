namespace Ydb.Sdk.Ado.Session;

internal interface ISessionSource
{
    ValueTask<ISession> OpenSession(CancellationToken cancellationToken);
}
