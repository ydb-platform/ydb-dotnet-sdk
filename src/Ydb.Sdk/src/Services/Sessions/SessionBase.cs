using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Services.Sessions;

public abstract class SessionBase : IDisposable
{
    protected readonly Driver Driver;
    internal static readonly TimeSpan DeleteSessionTimeout = TimeSpan.FromSeconds(1);

    public string Id { get; }
    internal string? Endpoint { get; }

    private protected bool Disposed;
    protected readonly ILogger Logger;

    protected SessionBase(Driver driver, string id, string? endpoint, ILogger logger)
    {
        Driver = driver;
        Id = id;
        Endpoint = endpoint;
        Logger = logger;
    }

    private protected void CheckSession()
    {
        if (Disposed)
        {
            throw new ObjectDisposedException(GetType().FullName);
        }
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected abstract void Dispose(bool disposing);
}
