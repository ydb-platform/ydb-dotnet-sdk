using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Globalization;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Reader;

internal readonly record struct ReaderStats(long PartitionSessionCount);

/// <summary>
/// Topic reader message and commit lifecycle metrics.
/// </summary>
internal sealed class ReaderMetricsReporter : IDisposable
{
    private static readonly List<ReaderMetricsReporter> Reporters = [];

    private static readonly Counter<long> ReceivedMessages;
    private static readonly Counter<long> DeliveredMessages;
    private static readonly Counter<long> CommitQueued;
    private static readonly Counter<long> CommitAcknowledged;
    private static long _nextReaderId;

    private readonly KeyValuePair<string, object?>[] _commonTags;
    private readonly Func<ReaderStats> _readerStats;

    static ReaderMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk.Topic", YdbSdkVersion.Value);

        meter.CreateObservableGauge(
            "ydb.topic.reader.partition_session.count",
            ObservePartitionSessionCount,
            unit: "{session}",
            description: "The number of partition sessions currently in the reader session processing lifecycle.");

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

    internal ReaderMetricsReporter(
        string endpoint,
        string database,
        string consumer,
        string? readerName,
        Func<ReaderStats> readerStats)
    {
        _readerStats = readerStats;
        _commonTags =
        [
            new KeyValuePair<string, object?>("endpoint", endpoint),
            new KeyValuePair<string, object?>("database", database),
            new KeyValuePair<string, object?>("consumer", consumer),
            new KeyValuePair<string, object?>("reader.name", string.IsNullOrEmpty(readerName)
                ? "reader-" + Interlocked.Increment(ref _nextReaderId).ToString(CultureInfo.InvariantCulture)
                : readerName)
        ];
        Register();
    }

    internal void ReportReceived(long messages, string topic) => Record(ReceivedMessages, messages, topic);

    internal void ReportDelivered(long messages, string topic) => Record(DeliveredMessages, messages, topic);

    internal void ReportCommitQueued(long messages, string topic) => Record(CommitQueued, messages, topic);

    internal void ReportCommitAcknowledged(long messages, string topic) =>
        Record(CommitAcknowledged, messages, topic);

    private void Register()
    {
        lock (Reporters)
        {
            Reporters.Add(this);
        }
    }

    public void Dispose()
    {
        lock (Reporters)
        {
            Reporters.Remove(this);
        }
    }

    private void Record(Counter<long> counter, long value, string topic)
    {
        if (!counter.Enabled || value <= 0)
        {
            return;
        }

        var tags = new TagList(_commonTags) { { "topic", topic } };
        counter.Add(value, tags);
    }

    private static IEnumerable<Measurement<long>> ObservePartitionSessionCount()
    {
        lock (Reporters)
        {
            return Reporters
                .Select(reader => new Measurement<long>(
                    reader._readerStats().PartitionSessionCount,
                    reader._commonTags))
                .ToArray();
        }
    }
}
