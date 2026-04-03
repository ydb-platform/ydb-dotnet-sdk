namespace Ydb.Sdk.Ado.Session;

internal interface ISessionSource : IAsyncDisposable
{
    ValueTask<ISession> OpenSession(CancellationToken cancellationToken);

    IDriver Driver { get; }

    YdbMetricsReporter MetricsReporter { get; }
    
    (int Idle, int Busy) Statistics { get; }
}
