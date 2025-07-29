using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Ado.Session;

internal class PoolingSessionFactory : IPoolingSessionFactory<PoolingSession>
{
    private readonly IDriver _driver;
    private readonly bool _disableServerBalancer;
    private readonly ILogger<PoolingSession> _logger;

    internal PoolingSessionFactory(IDriver driver, YdbConnectionStringBuilder settings, ILoggerFactory loggerFactory)
    {
        _driver = driver;
        _disableServerBalancer = settings.DisableServerBalancer;
        _logger = loggerFactory.CreateLogger<PoolingSession>();
    }

    public static async Task<PoolingSessionFactory> Create(YdbConnectionStringBuilder settings) =>
        new(await settings.BuildDriver(), settings, settings.LoggerFactory ?? NullLoggerFactory.Instance);

    public PoolingSession NewSession(PoolingSessionSource<PoolingSession> source) =>
        new(_driver, source, _disableServerBalancer, _logger);

    public ValueTask DisposeAsync() => _driver.DisposeAsync();
}
