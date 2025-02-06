using System.Text;

namespace Ydb.Sdk.Services.Topic.Reader;

internal class ReaderConfig
{
    public ReaderConfig(
        List<SubscribeSettings> subscribeSettings,
        string? consumerName,
        string? readerName,
        long memoryUsageMaxBytes)
    {
        SubscribeSettings = subscribeSettings;
        ConsumerName = consumerName;
        ReaderName = readerName;
        MemoryUsageMaxBytes = memoryUsageMaxBytes;
    }

    public List<SubscribeSettings> SubscribeSettings { get; }

    public string? ConsumerName { get; }

    public string? ReaderName { get; }

    public long MemoryUsageMaxBytes { get; }

    public override string ToString()
    {
        var toString = new StringBuilder().Append("SubscribeSettings: [")
            .Append(string.Join(", ", SubscribeSettings))
            .Append(']')
            .Append(", MemoryUsageMaxBytes: ")
            .Append(MemoryUsageMaxBytes);

        if (ConsumerName != null)
        {
            toString.Append(", ConsumerName: ").Append(ConsumerName);
        }

        if (ReaderName != null)
        {
            toString.Append(", ReaderName: ").Append(ReaderName);
        }

        return toString.ToString();
    }
}
