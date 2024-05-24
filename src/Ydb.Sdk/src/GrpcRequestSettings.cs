using System.Collections.Immutable;
using Google.Protobuf.WellKnownTypes;
using Ydb.Operations;

namespace Ydb.Sdk;

public class GrpcRequestSettings
{
    public string TraceId { get; set; } = string.Empty;
    public TimeSpan? TransportTimeout { get; set; }

    public ImmutableArray<string> CustomClientHeaders { get; set; }

    internal int NodeId { get; set; } = 0;

    internal Action<Grpc.Core.Metadata?> TrailersHandler { get; set; } = _ => { };
}

public class OperationSettings : GrpcRequestSettings
{
    public TimeSpan? OperationTimeout { get; set; }

    internal OperationParams MakeOperationParams()
    {
        var opParams = new OperationParams();

        if (OperationTimeout != null)
        {
            opParams.OperationTimeout = Duration.FromTimeSpan(OperationTimeout.Value);
        }

        return opParams;
    }
}
