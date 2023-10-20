namespace Ydb.Sdk;

public class RequestSettings
{
    public string TraceId { get; set; } = string.Empty;

    public TimeSpan? TransportTimeout { get; set; }
}

public class OperationRequestSettings : RequestSettings
{
    public TimeSpan? OperationTimeout { get; set; }
}
