using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Transport;

internal class AuthGrpcChannelTransport : GrpcTransport
{
    private readonly GrpcChannel _channel;

    public AuthGrpcChannelTransport(
        DriverConfig driverConfig,
        ILogger logger
    ) : base(
        new DriverConfig(
            endpoint: driverConfig.Endpoint,
            database: driverConfig.Database,
            customServerCertificate: driverConfig.CustomServerCertificate
        ), logger
    )
    {
        _channel = ChannelsCache.CreateChannel(Config.Endpoint, Config);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _channel.Dispose();
        }
    }

    protected override (string, GrpcChannel) GetChannel()
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
}
