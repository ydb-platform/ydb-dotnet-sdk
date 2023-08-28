namespace Ydb.Sdk
{
    public class RequestSettings
    {
        public string TraceId { get; set; } = String.Empty;

        public TimeSpan? TransportTimeout { get; set; }
    }

    public class OperationRequestSettings : RequestSettings
    {
        public TimeSpan? OperationTimeout { get; set; }
    }
}
