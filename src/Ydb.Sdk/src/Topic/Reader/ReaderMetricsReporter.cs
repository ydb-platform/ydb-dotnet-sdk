using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Reader;

internal interface IReaderMetricsSource
{
    long GetPartitionSessionCount(string topic);

    long GetCommitOffsetLagMax(string topic);
}

/// <summary>
/// Topic reader message and commit lifecycle metrics.
/// </summary>
internal sealed class ReaderMetricsReporter : IDisposable
{
    private static readonly List<ReaderMetricsReporter> Reporters = [];
    private static long _lastReaderId;

    private static readonly Counter<long> ReceivedMessages;
    private static readonly Counter<long> ReceivedBytes;
    private static readonly Counter<long> SessionErrors;
    private static readonly Counter<long> DeliveredMessages;
    private static readonly ObservableGauge<double> LocalBufferMessageAgeMax;
    private static readonly Counter<long> CommitQueued;
    private static readonly Counter<long> CommitAcknowledged;
    private static readonly ObservableGauge<long> CreditBalanceBytes;

    private readonly KeyValuePair<string, object?>[] _commonTags;
    private readonly IReaderMetricsSource _readerMetricsSource;
    private readonly ConcurrentDictionary<string, TopicMetrics> _topics = new();
    private long _creditBalanceBytes;

    static ReaderMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk.Topic", YdbSdkVersion.Value);

        meter.CreateObservableGauge(
            "ydb.topic.reader.partition_session.count",
            ObservePartitionSessionCount,
            unit: "{session}",
            description: "The number of partition sessions currently in the reader session processing lifecycle.");

        CreditBalanceBytes = meter.CreateObservableGauge(
            "ydb.topic.reader.credit_balance_bytes",
            ObserveCreditBalanceBytes,
            unit: "By",
            description: "The protocol credit granted to the server and not yet consumed by read responses.");

        LocalBufferMessageAgeMax = meter.CreateObservableGauge(
            "ydb.topic.reader.local_buffer.message_age.max",
            ObserveLocalBufferMessageAgeMax,
            unit: "s",
            description: "The age of the oldest batch retained in the reader's local buffer.");

        meter.CreateObservableGauge(
            "ydb.topic.reader.commit_offset.lag.max",
            ObserveCommitOffsetLag,
            unit: "{message}",
            description: "The maximum gap between requested and acknowledged commit offsets.");

        ReceivedMessages = meter.CreateCounter<long>(
            "ydb.topic.reader.received.messages",
            unit: "{message}",
            description: "The number of messages accepted by the SDK for an active partition session.");

        ReceivedBytes = meter.CreateCounter<long>(
            "ydb.topic.reader.received.bytes",
            unit: "By",
            description: "The protocol bytes_size received in read responses.");

        SessionErrors = meter.CreateCounter<long>(
            "ydb.topic.reader.session.errors",
            unit: "{error}",
            description: "The number of reader stream session errors by retry decision.");

        DeliveredMessages = meter.CreateCounter<long>(
            "ydb.topic.reader.delivered.messages",
            unit: "{message}",
            description: "The number of messages delivered by the SDK to application code.");

        meter.CreateObservableGauge(
            "ydb.topic.reader.local_buffer.messages",
            ObserveLocalBufferMessages,
            unit: "{message}",
            description: "The number of messages currently buffered by the reader.");

        CommitQueued = meter.CreateCounter<long>(
            "ydb.topic.reader.commit.queued",
            unit: "{message}",
            description: "The number of messages in commit ranges accepted by the SDK.");

        CommitAcknowledged = meter.CreateCounter<long>(
            "ydb.topic.reader.commit.acknowledged",
            unit: "{message}",
            description: "The number of messages in commit ranges completed by successful acknowledgements.");
    }

    private static string NextReaderName => $"reader-{Interlocked.Increment(ref _lastReaderId)}";

    internal ReaderMetricsReporter(
        string endpoint,
        string database,
        string? consumer,
        string? readerName,
        IEnumerable<string> topics,
        IReaderMetricsSource readerMetricsSource)
    {
        _readerMetricsSource = readerMetricsSource;
        var commonTags = new TagList
        {
            { "endpoint", endpoint },
            { "database", database }
        };
        if (consumer is not null)
        {
            commonTags.Add("consumer", consumer);
        }

        commonTags.Add("reader.name", readerName ?? NextReaderName);

        _commonTags = commonTags.ToArray();
        foreach (var topic in topics)
        {
            RegisterTopic(topic);
        }

        Register();
    }

    internal void ReportReceived(long messages, string topic) => Record(ReceivedMessages, messages, topic);

    internal static long ReportReadResponseStart() => LocalBufferMessageAgeMax.Enabled ? Stopwatch.GetTimestamp() : 0;

    internal void ReportReceivedBytes(long bytes) => ReceivedBytes.Add(bytes, _commonTags);

    internal void ReportCreditBalanceBytes(long bytes)
    {
        if (!CreditBalanceBytes.Enabled)
        {
            return;
        }

        Interlocked.Add(ref _creditBalanceBytes, bytes);
    }

    internal void ResetCreditBalanceBytes(long bytes = 0)
    {
        if (!CreditBalanceBytes.Enabled)
        {
            return;
        }

        Interlocked.Exchange(ref _creditBalanceBytes, bytes);
    }

    internal void ReportSessionError(StatusCode statusCode, bool retry = true) =>
        TopicMetricsUtils.ReportSessionError(SessionErrors, _commonTags, statusCode, retry);

    internal void ReportDelivered(long messages, string topic) => Record(DeliveredMessages, messages, topic);

    internal void RegisterTopic(string topic) =>
        _topics.GetOrAdd(topic, value => new TopicMetrics(new TagList(_commonTags) { { "topic", value } }.ToArray()));

    internal void ReportLocalBufferMessages(long messages, string topic) =>
        Interlocked.Add(ref _topics[topic].LocalBufferMessages, messages);

    internal void ResetLocalBufferMessages()
    {
        foreach (var topic in _topics.Values)
        {
            Interlocked.Exchange(ref topic.LocalBufferMessages, 0);
        }
    }

    internal void RegisterBufferedBatch(object batch, string topic, long receivedTimestamp) =>
        _topics[topic].BufferedBatches.TryAdd(batch, receivedTimestamp);

    internal void RemoveBufferedBatch(object batch, string topic) =>
        _topics[topic].BufferedBatches.TryRemove(batch, out _);

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

        foreach (var topic in _topics.Values)
        {
            topic.BufferedBatches.Clear();
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
                .SelectMany(reporter => reporter._topics.Select(topic =>
                    new Measurement<long>(reporter._readerMetricsSource.GetPartitionSessionCount(topic.Key),
                        topic.Value.Tags)))
                .ToArray();
        }
    }

    private static IEnumerable<Measurement<long>> ObserveCreditBalanceBytes()
    {
        lock (Reporters)
        {
            return Reporters
                .Select(reporter =>
                    new Measurement<long>(Interlocked.Read(ref reporter._creditBalanceBytes), reporter._commonTags))
                .ToArray();
        }
    }

    private static IEnumerable<Measurement<double>> ObserveLocalBufferMessageAgeMax()
    {
        lock (Reporters)
        {
            return Reporters.SelectMany(reporter => reporter._topics.Values.Select(topic =>
                    new Measurement<double>(topic.OldestAge, topic.Tags)))
                .ToArray();
        }
    }

    private static IEnumerable<Measurement<long>> ObserveLocalBufferMessages()
    {
        lock (Reporters)
        {
            return Reporters.SelectMany(reporter => reporter._topics.Values.Select(topic =>
                    new Measurement<long>(Interlocked.Read(ref topic.LocalBufferMessages), topic.Tags)))
                .ToArray();
        }
    }

    private static IEnumerable<Measurement<long>> ObserveCommitOffsetLag()
    {
        lock (Reporters)
        {
            return Reporters.SelectMany(reporter => reporter._topics.Select(topic =>
                    new Measurement<long>(reporter._readerMetricsSource.GetCommitOffsetLagMax(topic.Key),
                        topic.Value.Tags)))
                .ToArray();
        }
    }

    private sealed class TopicMetrics(KeyValuePair<string, object?>[] tags)
    {
        internal KeyValuePair<string, object?>[] Tags { get; } = tags;
        internal readonly ConcurrentDictionary<object, long> BufferedBatches = new();
        internal long LocalBufferMessages;

        internal double OldestAge
        {
            get
            {
                var oldest = BufferedBatches.Values.Where(timestamp => timestamp > 0).DefaultIfEmpty().Min();
                return oldest == 0 ? 0 : Stopwatch.GetElapsedTime(oldest).TotalSeconds;
            }
        }
    }
}
