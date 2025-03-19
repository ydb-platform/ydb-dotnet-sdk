using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Transport;

internal class AuthGrpcChannelDriver : BaseDriver
{
    private readonly GrpcChannel _channel;

    public AuthGrpcChannelDriver(
        DriverConfig driverConfig,
        GrpcChannelFactory grpcChannelFactory,
        ILoggerFactory loggerFactory
    ) : base(
        new DriverConfig(
            endpoint: driverConfig.Endpoint,
            database: driverConfig.Database,
            customServerCertificates: driverConfig.CustomServerCertificates
        ), loggerFactory, loggerFactory.CreateLogger<AuthGrpcChannelDriver>())
    {
        _channel = grpcChannelFactory.CreateChannel(Config.Endpoint);
    }

    protected override (string, GrpcChannel) GetChannel(long nodeId) => (Config.Endpoint, _channel);

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
