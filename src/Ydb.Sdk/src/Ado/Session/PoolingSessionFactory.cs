using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Session;

internal class PoolingSessionFactory : IPoolingSessionFactory<PoolingSession>
{
    private readonly IDriver _driver;
    private readonly bool _disableServerBalancer;
    private readonly ILogger<PoolingSession> _logger;

    internal PoolingSessionFactory(IDriver driver, YdbConnectionStringBuilder settings)
    {
        _driver = driver;
        _disableServerBalancer = settings.DisableServerBalancer;
        _logger = settings.LoggerFactory.CreateLogger<PoolingSession>();
    }

    public PoolingSession NewSession(PoolingSessionSource<PoolingSession> source) =>
        new(_driver, source, _disableServerBalancer, _logger);

    public ValueTask DisposeAsync() => _driver.DisposeAsync();
}
