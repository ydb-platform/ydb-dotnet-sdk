using static Ydb.Topic.StreamWriteMessage.Types.WriteResponse;

namespace Ydb.Sdk.GrpcWrappers.Topic.Writer.Write;

internal class WriteStatistics
{
    public TimeSpan PersistingTime { get; set; }
    public TimeSpan MinQueueWaitTime { get; set; }
    public TimeSpan MaxQueueWaitTime { get; set; }
    public TimeSpan TopicQuotaWaitTime { get; set; }
        
    public static WriteStatistics FromProto(Types.WriteStatistics statistics)
    {
        return new WriteStatistics
        {
            PersistingTime = statistics.PersistingTime.ToTimeSpan(),
            MinQueueWaitTime = statistics.MinQueueWaitTime.ToTimeSpan(),
            MaxQueueWaitTime = statistics.MaxQueueWaitTime.ToTimeSpan(),
            TopicQuotaWaitTime = statistics.TopicQuotaWaitTime.ToTimeSpan()
        };
    }
}
