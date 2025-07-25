using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Session;

internal class PoolingSessionFactory : IPoolingSessionFactory
{
    private readonly IDriver _driver;
    private readonly bool _disableServerBalancer;
    private readonly ILogger<PoolingSession> _logger;

    public PoolingSessionFactory(IDriver driver, YdbConnectionStringBuilder settings, ILoggerFactory loggerFactory)
    {
        _driver = driver;
        _disableServerBalancer = settings.DisableServerBalancer;
        _logger = loggerFactory.CreateLogger<PoolingSession>();
    }

    public PoolingSessionBase NewSession(PoolingSessionSource source) =>
        new PoolingSession(_driver, source, _disableServerBalancer, _logger);
}
