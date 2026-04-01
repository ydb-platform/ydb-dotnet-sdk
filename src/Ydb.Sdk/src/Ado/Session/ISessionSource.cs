namespace Ydb.Sdk.Ado.Session;

internal interface ISessionSource : IAsyncDisposable
{
    ValueTask<ISession> OpenSession(CancellationToken cancellationToken);

    IDriver Driver { get; }

    (int Idle, int Busy) Statistics { get; }

    // YdbMetricsReporter? MetricsReporter { get; }
}
