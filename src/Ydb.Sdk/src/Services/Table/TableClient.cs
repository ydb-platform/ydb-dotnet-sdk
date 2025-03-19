using Ydb.Sdk.Services.Sessions;

namespace Ydb.Sdk.Services.Table;

public class TableClientConfig
{
    public SessionPoolConfig SessionPoolConfig { get; }

    public TableClientConfig(
        SessionPoolConfig? sessionPoolConfig = null)
    {
        SessionPoolConfig = sessionPoolConfig ?? new SessionPoolConfig();
    }
}

public partial class TableClient : IDisposable
{
    private readonly ISessionPool<Session> _sessionPool;
    private readonly Driver _driver;

    private bool _disposed;

    public TableClient(Driver driver, TableClientConfig? config = null)
    {
        config ??= new TableClientConfig();

        _driver = driver;
        _sessionPool = new SessionPool(driver, config.SessionPoolConfig);
    }

    internal TableClient(Driver driver, ISessionPool<Session> sessionPool)
    {
        _driver = driver;
        _sessionPool = sessionPool;
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private void Dispose(bool disposing)
    {
        if (_disposed)
        {
            return;
        }

        if (disposing)
        {
            _sessionPool.Dispose();
        }

        _disposed = true;
    }

    internal string MakeTablePath(string path) => path.StartsWith('/') ? path : $"{_driver.Database}/{path}";
}
