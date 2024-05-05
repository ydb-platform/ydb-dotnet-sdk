using Google.Protobuf.WellKnownTypes;
using Ydb.Operations;

namespace Ydb.Sdk;

public class RequestSettings
{
    public string TraceId { get; set; } = string.Empty;

    public TimeSpan? TransportTimeout { get; set; }
}

public class OperationRequestSettings : RequestSettings
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
