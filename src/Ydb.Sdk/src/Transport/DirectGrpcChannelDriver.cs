using Grpc.Core;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Transport;

public class DirectGrpcChannelDriver : BaseDriver
{
    private readonly GrpcChannel _channel;

    internal DirectGrpcChannelDriver(
        DriverConfig driverConfig,
        GrpcChannelFactory grpcChannelFactory,
        ILoggerFactory loggerFactory
    ) : base(
        new DriverConfig(
            endpoint: driverConfig.Endpoint,
            database: driverConfig.Database,
            customServerCertificates: driverConfig.CustomServerCertificates
        ), loggerFactory, loggerFactory.CreateLogger<DirectGrpcChannelDriver>())
    {
        _channel = grpcChannelFactory.CreateChannel(Config.Endpoint);
    }

    public DirectGrpcChannelDriver(DriverConfig driverConfig, ILoggerFactory loggerFactory) :
        this(driverConfig, new GrpcChannelFactory(loggerFactory, driverConfig), loggerFactory)
    {
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

    protected override ICredentialsProvider? CredentialsProvider => null;

    protected override async ValueTask InternalDispose()
    {
        await _channel.ShutdownAsync();

        _channel.Dispose();
    }
}
