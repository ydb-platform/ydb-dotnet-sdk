using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Writer;

internal interface IWriterMetricsSource
{
    long BufferUsed { get; }

    long BufferLimit { get; }
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
        MessageAckDuration = meter.CreateHistogram(
            "ydb.topic.writer.message.ack.duration",
            unit: "s",
            description: "Time from the first send of a message to its server acknowledgement.",
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

    internal static long ReportMessageSendStart() => MessageAckDuration.Enabled ? Stopwatch.GetTimestamp() : 0;

    internal void ReportMessageAckDuration(MessageSending message)
    {
        var startTimestamp = message.FirstSendTimestamp;
        if (startTimestamp == 0)
        {
            return;
        }

        message.FirstSendTimestamp = 0;
        MessageAckDuration.Record(Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds, _commonTags);
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
}
