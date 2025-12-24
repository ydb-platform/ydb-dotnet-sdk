using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Session;

internal class PoolingSessionFactory : IPoolingSessionFactory<PoolingSession>
{
    private readonly bool _disableServerBalancer;
    private readonly ILogger<PoolingSession> _logger;

    internal PoolingSessionFactory(IDriver driver, YdbConnectionStringBuilder settings)
    {
        Driver = driver;
        _disableServerBalancer = settings.DisableServerBalancer;
        _logger = settings.LoggerFactory.CreateLogger<PoolingSession>();
    }

    public PoolingSession NewSession(PoolingSessionSource<PoolingSession> source) =>
        new(Driver, source, _disableServerBalancer, _logger);

    public IDriver Driver { get; }

    public ValueTask DisposeAsync() => Driver.DisposeAsync();
}
