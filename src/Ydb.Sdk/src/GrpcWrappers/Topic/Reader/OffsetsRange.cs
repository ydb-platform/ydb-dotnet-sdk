namespace Ydb.Sdk.GrpcWrappers.Topic.Reader;

internal class OffsetsRange
{
    public long Start { get; set; }
    public long End { get; set; }

    public static OffsetsRange FromProto(Ydb.Topic.OffsetsRange range)
    {
        return new OffsetsRange
        {
            Start = range.Start,
            End = range.End
        };
    }

    public Ydb.Topic.OffsetsRange ToProto()
    {
        return new Ydb.Topic.OffsetsRange
        {
            Start = Start,
            End = End
        };
    }
}
