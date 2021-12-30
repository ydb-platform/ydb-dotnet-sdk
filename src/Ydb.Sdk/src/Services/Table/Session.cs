#if NETCOREAPP3_1
using System;
using System.Threading.Tasks;
#endif
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Table
{
    public partial class Session : ClientBase, IDisposable
    {
        internal static readonly TimeSpan DeleteSessionTimeout = TimeSpan.FromSeconds(1);

        private readonly SessionPool? _sessionPool = null;
        private readonly ILogger _logger;
        private bool _disposed = false;

        internal Session(Driver driver, SessionPool? sessionPool, string id, string? endpoint)
            : base(driver)
        {
            _sessionPool = sessionPool;
            _logger = Driver.LoggerFactory.CreateLogger<Session>();
            Id = id;
            Endpoint = endpoint;
        }

        public string Id { get; }

        internal string? Endpoint { get; }

        private void CheckSession()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        private void OnResponseStatus(Status status)
        {
            if (status.StatusCode == StatusCode.BadSession || status.StatusCode == StatusCode.SessionBusy)
            {
                if (_sessionPool != null)
                {
                    _sessionPool.InvalidateSession(Id);
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (_disposed)
            {
                return;
            }

            if (disposing)
            {
                if (_sessionPool is null)
                {
                    _logger.LogTrace($"Closing detached session on dispose: {Id}");

                    var client = new TableClient(Driver, new NoPool());
                    _ = client.DeleteSession(Id, new DeleteSessionSettings
                    {
                        TransportTimeout = DeleteSessionTimeout
                    });
                } else
                {
                    _sessionPool.ReturnSession(Id);
                }
            }

            _disposed = true;
        }

        internal Task<Driver.UnaryResponse<TResponse>> UnaryCall<TRequest, TResponse>(
            Grpc.Core.Method<TRequest, TResponse> method,
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
}
