using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Services.Sessions;

public abstract class SessionBase : IDisposable
{
    internal static readonly TimeSpan DeleteSessionTimeout = TimeSpan.FromSeconds(1);
    protected readonly Driver Driver;
    protected readonly ILogger Logger;

    private protected bool Disposed;

    protected SessionBase(Driver driver, string id, long nodeId, ILogger logger)
    {
        Driver = driver;
        Id = id;
        NodeId = nodeId;
        Logger = logger;
    }

    public string Id { get; }
    public long NodeId { get; }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    private protected void CheckSession()
    {
        if (Disposed) throw new ObjectDisposedException(GetType().FullName);
    }

    protected abstract void Dispose(bool disposing);
}
