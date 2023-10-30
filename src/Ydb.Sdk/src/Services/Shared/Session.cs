using Microsoft.Extensions.Logging;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Shared;

public abstract class Session : ClientBase, IDisposable
{
    internal static readonly TimeSpan DeleteSessionTimeout = TimeSpan.FromSeconds(1);


    public string Id { get; }
    internal string? Endpoint { get; }

    private protected bool Disposed;
    protected readonly ILogger Logger;


    protected Session(Driver driver, string id, string? endpoint, ILogger logger) : base(driver)
    {
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
    }

    protected abstract void Dispose(bool disposing);
}