using Grpc.Core;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Services.Sessions;

namespace Ydb.Sdk.Services.Table;

public partial class Session : SessionBase
{
    private readonly SessionPool? _sessionPool;

    internal Session(Driver driver, SessionPool? sessionPool, string id, string? endpoint)
        : base(driver, id, endpoint, driver.LoggerFactory.CreateLogger<Session>())
    {
        _sessionPool = sessionPool;
    }


    private void OnResponseStatus(Status status)
    {
        if (status.StatusCode is StatusCode.BadSession or StatusCode.SessionBusy)
        {
            _sessionPool?.InvalidateSession(Id);
        }
    }


    protected override void Dispose(bool disposing)
    {
        if (Disposed)
        {
            return;
        }

        if (disposing)
        {
            if (_sessionPool is null)
            {
                Logger.LogTrace($"Closing detached session on dispose: {Id}");

                var client = new TableClient(Driver, new NoPool<Session>());
                _ = client.DeleteSession(Id, new DeleteSessionSettings
                {
                    TransportTimeout = DeleteSessionTimeout
                });
            }
            else
            {
                _sessionPool.ReturnSession(Id);
            }
        }

        Disposed = true;
    }

    internal Task<Driver.UnaryResponse<TResponse>> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        RequestSettings settings)
        where TRequest : class
        where TResponse : class
    {
        return Driver.UnaryCall(
            method: method,
            request: request,
            settings: settings,
            preferredEndpoint: Endpoint
        );
    }
}
