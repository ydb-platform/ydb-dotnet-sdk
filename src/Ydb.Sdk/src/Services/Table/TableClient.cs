using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Shared;

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

public partial class TableClient : ClientBase, IDisposable
{
    private readonly ISessionPool<Session> _sessionPool;
    private bool _disposed;

    public TableClient(Driver driver, TableClientConfig? config = null)
        : base(driver)
    {
        config ??= new TableClientConfig();

        _sessionPool = new SessionPool(
            driver: driver,
            config: config.SessionPoolConfig);
    }

    internal TableClient(Driver driver, ISessionPool<Session> sessionPool)
        : base(driver)
    {
        _sessionPool = sessionPool;
    }

    public void Dispose()
    {
        Dispose(true);
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
}
