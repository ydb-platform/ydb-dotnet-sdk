using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Transport;

internal class AuthGrpcChannelTransport : BaseDriver
{
    private readonly GrpcChannel _channel;

    public AuthGrpcChannelTransport(
        DriverConfig driverConfig,
        GrpcChannelFactory grpcChannelFactory,
        ILoggerFactory loggerFactory
    ) : base(
        new DriverConfig(
            endpoint: driverConfig.Endpoint,
            database: driverConfig.Database,
            customServerCertificate: driverConfig.CustomServerCertificate
        ), loggerFactory, loggerFactory.CreateLogger<AuthGrpcChannelTransport>())
    {
        _channel = grpcChannelFactory.CreateChannel(Config.Endpoint);
    }

    protected override (string, GrpcChannel) GetChannel(long nodeId)
    {
        return (Config.Endpoint, _channel);
    }

    protected override void OnRpcError(string endpoint, RpcException e)
    {
        var status = e.Status;
        if (e.Status.StatusCode != Grpc.Core.StatusCode.OK)
        {
            Logger.LogWarning("gRPC error {StatusCode}[{Detail}] on fixed channel {Endpoint}",
                status.StatusCode, status.Detail, endpoint);
        }
    }

    protected override async ValueTask InternalDispose()
    {
        await _channel.ShutdownAsync();

        _channel.Dispose();
    }
}
