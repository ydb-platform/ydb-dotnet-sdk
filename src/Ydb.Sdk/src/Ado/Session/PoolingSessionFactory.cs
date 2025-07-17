using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Ado.Session;

internal class PoolingSessionFactory : IPoolingSessionFactory
{
    private readonly IDriver _driver;
    private readonly YdbConnectionStringBuilder _settings;
    private readonly ILogger<PoolingSession> _logger;

    public PoolingSessionFactory(IDriver driver, YdbConnectionStringBuilder settings, ILogger<PoolingSession> logger)
    {
        _driver = driver;
        _settings = settings;
        _logger = logger;
    }

    public IPoolingSession NewSession(PoolingSessionSource source) =>
        new PoolingSession(_driver, source, _settings, _logger);
}
