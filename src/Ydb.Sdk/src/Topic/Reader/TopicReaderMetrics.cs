using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Reader;

/// <summary>
/// Topic reader message and commit lifecycle metrics.
/// </summary>
internal sealed class TopicReaderMetrics(string endpoint, string database, string consumer)
{
    private static readonly Counter<long> ReceivedMessages;
    private static readonly Counter<long> DeliveredMessages;
    private static readonly Counter<long> CommitQueued;
    private static readonly Counter<long> CommitAcknowledged;

    static TopicReaderMetrics()
    {
        var meter = new Meter("Ydb.Sdk", YdbSdkVersion.Value);

        ReceivedMessages = meter.CreateCounter<long>(
            "ydb.topic.reader.received.messages",
            unit: "{message}",
            description: "The number of messages accepted by the SDK for an active partition session.");

        DeliveredMessages = meter.CreateCounter<long>(
            "ydb.topic.reader.delivered.messages",
            unit: "{message}",
            description: "The number of messages delivered by the SDK to application code.");

        CommitQueued = meter.CreateCounter<long>(
            "ydb.topic.reader.commit.queued",
            unit: "{message}",
            description: "The number of messages in commit ranges accepted by the SDK.");

        CommitAcknowledged = meter.CreateCounter<long>(
            "ydb.topic.reader.commit.acknowledged",
            unit: "{message}",
            description: "The number of messages in commit ranges completed by successful acknowledgements.");
    }

    internal void ReportReceived(long messages, string topic) => Record(ReceivedMessages, messages, topic);

    internal void ReportDelivered(long messages, string topic) => Record(DeliveredMessages, messages, topic);

    internal void ReportCommitQueued(long messages, string topic) => Record(CommitQueued, messages, topic);

    internal void ReportCommitAcknowledged(long messages, string topic) =>
        Record(CommitAcknowledged, messages, topic);

    private void Record(Counter<long> counter, long value, string topic)
    {
        if (!counter.Enabled || value <= 0)
        {
            return;
        }

        var tags = new TagList
        {
            { "endpoint", endpoint },
            { "database", database },
            { "topic", topic },
            { "consumer", consumer }
        };
        counter.Add(value, tags);
    }
}
