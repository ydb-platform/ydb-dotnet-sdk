using System.Diagnostics;
using Google.Protobuf.WellKnownTypes;
using Ydb.Operations;

namespace Ydb.Sdk;

public class GrpcRequestSettings
{
    public string TraceId { get; init; } = string.Empty;
    public TimeSpan TransportTimeout { get; init; } = TimeSpan.Zero;
    public CancellationToken CancellationToken = CancellationToken.None;

    internal Activity? DbActivity { get; init; }
    internal List<string> ClientCapabilities { get; } = new();
    internal long NodeId { get; set; }
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
