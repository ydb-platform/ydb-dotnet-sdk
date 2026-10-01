using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Writer;

internal interface IWriterMetricsSource
{
    long BufferUsed { get; }

    long BufferLimit { get; }

    long OldestMessageTimestamp { get; }
}

internal sealed class WriterMetricsReporter : IDisposable
{
    private static readonly List<WriterMetricsReporter> Reporters = [];
    private static readonly HashSet<string> ActiveWriterNames = new(StringComparer.Ordinal);

    private static readonly Counter<long> WrittenMessages;
    private static readonly Counter<long> SendingMessages;
    private static readonly Counter<long> SendingBytes;
    private static readonly Counter<long> SessionErrors;
    private static readonly Histogram<double> MessageAckDuration;
    private static readonly ObservableGauge<double> SendingOldestAge;
    private static readonly Histogram<double> BufferWaitDuration;

    private readonly KeyValuePair<string, object?>[] _commonTags;
    private readonly IWriterMetricsSource _writerMetricsSource;
    private readonly string _writerName;

    static WriterMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk.Topic", YdbSdkVersion.Value);

        WrittenMessages = meter.CreateCounter<long>(
            "ydb.topic.writer.written.messages",
            unit: "{message}",
            description: "The number of messages confirmed written by the server, including already written messages.");
        SendingMessages = meter.CreateCounter<long>(
            "ydb.topic.writer.sending.messages",
            unit: "{message}",
            description: "The number of messages accepted by the SDK for sending.");
        SendingBytes = meter.CreateCounter<long>(
            "ydb.topic.writer.sending.bytes",
            unit: "By",
            description: "The uncompressed body size of messages accepted by the writer.");
        SessionErrors = meter.CreateCounter<long>(
            "ydb.topic.writer.session.errors",
            unit: "{error}",
            description: "The number of writer stream session errors by retry decision.");
        meter.CreateObservableGauge("ydb.topic.writer.buffer.used.bytes", ObserveBufferUsed,
            unit: "By", description: "The occupied budget of the writer buffer limiter.");
        meter.CreateObservableGauge("ydb.topic.writer.buffer.limit.bytes", ObserveBufferLimit,
            unit: "By", description: "The configured limit of the writer buffer limiter.");
        SendingOldestAge = meter.CreateObservableGauge(
            "ydb.topic.writer.sending.oldest_age",
            ObserveSendingOldestAge,
            unit: "s",
            description: "The age of the oldest message in the writer's in-flight buffer.");
        MessageAckDuration = meter.CreateHistogram(
            "ydb.topic.writer.message.ack.duration",
            unit: "s",
            description: "Time from accepting a message into the send buffer to its server acknowledgement.",
            advice: new InstrumentAdvice<double>
                { HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10] });
        BufferWaitDuration = meter.CreateHistogram(
            "ydb.topic.writer.buffer.wait.duration",
            unit: "s",
            description: "Time waiting for buffer capacity before a message is accepted.",
            advice: new InstrumentAdvice<double>
                { HistogramBucketBoundaries = [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10] });
    }

    internal WriterMetricsReporter(string endpoint, string database, string topic, string writerName,
        IWriterMetricsSource writerMetricsSource)
    {
        _writerMetricsSource = writerMetricsSource;
        lock (Reporters)
        {
            if (!ActiveWriterNames.Add(writerName))
            {
                throw new ArgumentException("WriterName must be unique among active writers.", nameof(writerName));
            }

            _writerName = writerName;
            _commonTags =
            [
                new KeyValuePair<string, object?>("endpoint", endpoint),
                new KeyValuePair<string, object?>("database", database),
                new KeyValuePair<string, object?>("topic", topic),
                new KeyValuePair<string, object?>("writer.name", writerName)
            ];
            Reporters.Add(this);
        }
    }

    internal void ReportWritten() => WrittenMessages.Add(1, _commonTags);

    internal void ReportSending() => SendingMessages.Add(1, _commonTags);

    internal void ReportSendingBytes(long bytes) => SendingBytes.Add(bytes, _commonTags);

    internal void ReportSessionError(StatusCode statusCode, bool retry = true) =>
        TopicMetricsUtils.ReportSessionError(SessionErrors, _commonTags, statusCode, retry);

    internal static long ReportMessageSendStart() =>
        MessageAckDuration.Enabled || SendingOldestAge.Enabled ? Stopwatch.GetTimestamp() : 0;

    internal static long ReportBufferWaitStart() =>
        BufferWaitDuration.Enabled ? Stopwatch.GetTimestamp() : 0;

    internal void ReportMessageAckDuration(long startTimestamp)
    {
        if (startTimestamp == 0)
        {
            return;
        }

        MessageAckDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, _commonTags);
    }

    internal void ReportBufferWaitDuration(long startTimestamp)
    {
        if (startTimestamp == 0)
        {
            return;
        }

        BufferWaitDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, _commonTags);
    }

    public void Dispose()
    {
        lock (Reporters)
        {
            if (Reporters.Remove(this))
            {
                ActiveWriterNames.Remove(_writerName);
            }
        }
    }

    private static IEnumerable<Measurement<long>> ObserveBufferUsed()
    {
        lock (Reporters)
        {
            return Reporters
                .Select(reporter =>
                    new Measurement<long>(reporter._writerMetricsSource.BufferUsed, reporter._commonTags))
                .ToArray();
        }
    }

    private static IEnumerable<Measurement<long>> ObserveBufferLimit()
    {
        lock (Reporters)
        {
            return Reporters
                .Select(reporter =>
                    new Measurement<long>(reporter._writerMetricsSource.BufferLimit, reporter._commonTags))
                .ToArray();
        }
    }

    private static IEnumerable<Measurement<double>> ObserveSendingOldestAge()
    {
        lock (Reporters)
        {
            return Reporters.Select(reporter =>
            {
                var timestamp = reporter._writerMetricsSource.OldestMessageTimestamp;
                return new Measurement<double>(
                    timestamp == 0 ? 0 : Stopwatch.GetElapsedTime(timestamp).TotalSeconds,
                    reporter._commonTags);
            }).ToArray();
        }
    }
}
