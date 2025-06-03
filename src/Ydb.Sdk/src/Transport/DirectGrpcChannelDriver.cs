using Grpc.Core;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Transport;

public class DirectGrpcChannelDriver : BaseDriver
{
    public DirectGrpcChannelDriver(DriverConfig driverConfig, ILoggerFactory loggerFactory) :
        base(driverConfig, loggerFactory, loggerFactory.CreateLogger<DirectGrpcChannelDriver>())
    {
    }

    protected override string GetEndpoint(long nodeId) => Config.Endpoint;

    protected override void OnRpcError(string endpoint, RpcException e)
    {
        var status = e.Status;

        Logger.LogWarning("gRPC error {StatusCode}[{Detail}] on fixed channel {Endpoint}",
            status.StatusCode, status.Detail, endpoint);
    }
}
