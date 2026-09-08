using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Reader;

internal interface IReaderMetricsSource
{
    long PartitionSessionCount { get; }

    double LocalBufferMessageAgeMax { get; }

    IReadOnlyDictionary<long, PartitionSession>? PartitionSessions { get; }
}

/// <summary>
/// Topic reader message and commit lifecycle metrics.
/// </summary>
internal sealed class ReaderMetricsReporter : IDisposable
{
    private static readonly List<ReaderMetricsReporter> Reporters = [];

    private static readonly Counter<long> ReceivedMessages;
    private static readonly Counter<long> ReceivedBytes;
    private static readonly Counter<long> SessionErrors;
    private static readonly Counter<long> DeliveredMessages;
    private static readonly UpDownCounter<long> LocalBufferMessages;
    private static readonly ObservableGauge<double> LocalBufferMessageAgeMax;
    private static readonly Counter<long> CommitQueued;
    private static readonly Counter<long> CommitAcknowledged;
    private static readonly ObservableGauge<long> CreditBalanceBytes;

    private readonly KeyValuePair<string, object?>[] _commonTags;
    private readonly IReaderMetricsSource _readerMetricsSource;
    private long _creditBalanceBytes;
    private readonly string _endpoint;
    private readonly string _database;
    private readonly string? _consumer;
    private readonly string? _readerName;
    private readonly string[] _topics;

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

        LocalBufferMessages = meter.CreateUpDownCounter<long>(
            "ydb.topic.reader.local_buffer.messages",
            unit: "{message}",
            description: "The number of messages accepted by the SDK but not yet delivered.");

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
        string? consumer,
        string? readerName,
        IEnumerable<string> topics,
        IReaderMetricsSource readerMetricsSource)
    {
        _readerMetricsSource = readerMetricsSource;
        _endpoint = endpoint;
        _database = database;
        _consumer = consumer;
        _readerName = readerName;
        _topics = topics.Distinct().ToArray();
        var commonTags = new TagList
        {
            { "endpoint", endpoint },
            { "database", database }
        };
        if (consumer is not null)
        {
            commonTags.Add("consumer", consumer);
        }

        if (readerName is not null)
        {
            commonTags.Add("reader.name", readerName);
        }

        _commonTags = commonTags.ToArray();
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

    internal void ReportSessionError(StatusCode statusCode, bool retry = true)
    {
        if (!SessionErrors.Enabled)
        {
            return;
        }

        SessionErrors.Add(1, new TagList(_commonTags)
        {
            { "retry_decision", retry ? "retry" : "stop" },
            { "status_code", statusCode.ToString() },
            {
                "error.type", statusCode switch
                {
                    StatusCode.Unspecified => "session_closed",
                    _ when statusCode.IsTransportError() => "transport_error",
                    _ => "ydb_error"
                }
            }
        });
    }

    internal void ReportDelivered(long messages, string topic) => Record(DeliveredMessages, messages, topic);

    internal void ReportLocalBuffer(long messages, string topic)
    {
        if (!LocalBufferMessages.Enabled)
        {
            return;
        }

        var tags = new TagList(_commonTags) { { "topic", topic } };
        LocalBufferMessages.Add(messages, tags);
    }

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
                .Select(reporter =>
                    new Measurement<long>(reporter._readerMetricsSource.PartitionSessionCount, reporter._commonTags))
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
            return Reporters.Select(reporter =>
                    new Measurement<double>(reporter._readerMetricsSource.LocalBufferMessageAgeMax,
                        reporter._commonTags))
                .ToArray();
        }
    }

    private static IEnumerable<Measurement<long>> ObserveCommitOffsetLag()
    {
        lock (Reporters)
        {
            var maxByTags = new Dictionary<
                (string Endpoint, string Database, string Consumer, string? ReaderName, string Topic),
                long>();

            foreach (var reporter in Reporters)
            {
                if (reporter._consumer is null)
                {
                    continue;
                }

                var topicLags = reporter._topics.ToDictionary(topic => topic, _ => 0L);
                if (reporter._readerMetricsSource.PartitionSessions is { } partitionSessions)
                {
                    foreach (var (_, partitionSession) in partitionSessions)
                    {
                        topicLags[partitionSession.TopicPath] = Math.Max(
                            topicLags.GetValueOrDefault(partitionSession.TopicPath),
                            partitionSession.CommitOffsetLag);
                    }
                }

                foreach (var (topic, lag) in topicLags)
                {
                    var key = (reporter._endpoint, reporter._database, reporter._consumer,
                        reporter._readerName, topic);
                    maxByTags[key] = Math.Max(maxByTags.GetValueOrDefault(key), lag);
                }
            }

            return maxByTags.Select(pair =>
            {
                var tags = new TagList
                {
                    { "endpoint", pair.Key.Endpoint },
                    { "database", pair.Key.Database },
                    { "consumer", pair.Key.Consumer }
                };
                if (pair.Key.ReaderName is not null)
                {
                    tags.Add("reader.name", pair.Key.ReaderName);
                }

                tags.Add("topic", pair.Key.Topic);
                return new Measurement<long>(pair.Value, tags.ToArray());
            }).ToArray();
        }
    }
}
