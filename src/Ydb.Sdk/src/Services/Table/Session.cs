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

    private void OnResponseTrailers(Grpc.Core.Metadata? trailers)
    {
        if (trailers is null)
        {
            return;
        }

        foreach (var hint in trailers.GetAll(Metadata.RpcServerHintsHeader))
        {
            if (hint.Value == Metadata.GracefulShutdownHint)
            {
                _sessionPool?.InvalidateSession(Id);
            }
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
                var task = client.DeleteSession(Id, new DeleteSessionSettings
                {
                    TransportTimeout = DeleteSessionTimeout
                });
                task.Wait();
            }
            else
            {
                _sessionPool.ReturnSession(Id);
            }
        }

        Disposed = true;
    }

    internal async Task<Driver.UnaryResponse<TResponse>> UnaryCall<TRequest, TResponse>(
        Method<TRequest, TResponse> method,
        TRequest request,
        RequestSettings settings)
        where TRequest : class
        where TResponse : class
    {
        var response = await Driver.UnaryCall(
            method: method,
            request: request,
            settings: settings,
            preferredEndpoint: Endpoint
        );
        OnResponseTrailers(response.Trailers);
        return response;
    }
}
