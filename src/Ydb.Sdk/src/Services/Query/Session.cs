using Microsoft.Extensions.Logging;
using Ydb.Sdk.Services.Sessions;

namespace Ydb.Sdk.Services.Query;

/// <summary>
/// Sessions are basic primitives for communicating with YDB Query Service. They are similar to
/// connections for classic relational DBs. Sessions serve three main purposes:
/// 1. Provide a flow control for DB requests with limited number of active channels.
/// 2. Distribute load evenly across multiple DB nodes.
/// 3. Store state for volatile stateful operations, such as short-living transactions.
/// </summary>
public class Session : SessionBase
{
    private readonly SessionPool _sessionPool;

    internal Session(Driver driver, SessionPool sessionPool, string id, long nodeId) :
        base(driver, id, nodeId, driver.LoggerFactory.CreateLogger<Session>())
    {
        _sessionPool = sessionPool;
    }

    protected override void Dispose(bool disposing)
    {
        if (Disposed)
        {
            return;
        }

        if (disposing)
        {
            _sessionPool.ReturnSession(Id);
        }

        Disposed = true;
    }
}
