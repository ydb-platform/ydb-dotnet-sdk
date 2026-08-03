using System.Text;

namespace Ydb.Sdk.Topic.Reader;

internal class ReaderConfig(
    List<SubscribeSettings> subscribeSettings,
    string? consumerName,
    string? readerName,
    long memoryUsageMaxBytes)
{
    public List<SubscribeSettings> SubscribeSettings { get; } = subscribeSettings;

    public string? ConsumerName { get; } = consumerName;

    public string? ReaderName { get; } = readerName;

    public long MemoryUsageMaxBytes { get; } = memoryUsageMaxBytes;

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
