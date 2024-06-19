using Google.Protobuf.WellKnownTypes;

namespace Ydb.Sdk.GrpcWrappers.Topic.Extensions;

public static class TimeExtensions
{
    public static TimeSpan? ToOptionalTimeSpan(this Duration? duration) => duration?.ToTimeSpan();

    public static Duration? ToDuration(this TimeSpan? timespan) => 
        timespan == null ? default : Duration.FromTimeSpan(timespan.Value);

    public static Timestamp? ToTimestamp(this DateTime? dateTime) =>
        dateTime == null ? default : Timestamp.FromDateTime(dateTime.Value);
}