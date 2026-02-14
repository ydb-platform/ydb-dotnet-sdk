using Grpc.Core;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Transport;

public class DirectGrpcChannelDriver : BaseDriver
{
    public DirectGrpcChannelDriver(DriverConfig driverConfig, ILoggerFactory loggerFactory) :
        base(driverConfig, loggerFactory, loggerFactory.CreateLogger<DirectGrpcChannelDriver>())
    {
    }

    protected override EndpointInfo GetEndpoint(long nodeId) => Config.EndpointInfo;

    protected override void OnRpcError(EndpointInfo endpointInfo, RpcException e)
    {
        var status = e.Status;

        Logger.LogWarning("gRPC error {StatusCode}[{Detail}] on fixed channel {Endpoint}",
            status.StatusCode, status.Detail, endpointInfo.Endpoint);
    }
}
