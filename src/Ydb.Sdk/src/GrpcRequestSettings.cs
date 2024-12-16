using System.Collections.Immutable;
using Google.Protobuf.WellKnownTypes;
using Ydb.Operations;

namespace Ydb.Sdk;

public class GrpcRequestSettings
{
    internal static readonly GrpcRequestSettings DefaultInstance = new();

    public string TraceId { get; set; } = string.Empty;
    public TimeSpan TransportTimeout { get; set; } = TimeSpan.Zero;
    public ImmutableArray<string> CustomClientHeaders { get; } = new();

    internal long NodeId { get; set; }
    internal Action<Grpc.Core.Metadata> TrailersHandler { get; set; } = _ => { };
}

public class OperationSettings : GrpcRequestSettings
{
    public TimeSpan? OperationTimeout { get; set; }

    public TimeSpan? CancelTimeout { get; set; }

    public bool IsAsyncMode { get; set; }

    public bool ReportCostInfo { get; set; }

    internal OperationParams MakeOperationParams()
    {
        var opParams = new OperationParams();

        if (OperationTimeout != null)
        {
            opParams.OperationTimeout = Duration.FromTimeSpan(OperationTimeout.Value);
        }

        if (CancelTimeout != null)
        {
            opParams.CancelAfter = Duration.FromTimeSpan(CancelTimeout.Value);
        }

        if (IsAsyncMode)
        {
            opParams.OperationMode = OperationParams.Types.OperationMode.Async;
        }

        if (ReportCostInfo)
        {
            opParams.ReportCostInfo = FeatureFlag.Types.Status.Enabled;
        }

        return opParams;
    }
}
