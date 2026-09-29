using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Writer;

internal interface IWriterMetricsSource
{
    long BufferUsed { get; }
}

internal sealed class WriterMetricsReporter : IDisposable
{
    private static readonly List<WriterMetricsReporter> Reporters = [];
    private static long _lastWriterId;

    private static readonly Counter<long> WrittenMessages;
    private static readonly Counter<long> SendingMessages;
    private static readonly Counter<long> SendingBytes;
    private static readonly Counter<long> SessionErrors;

    private readonly KeyValuePair<string, object?>[] _commonTags;
    private readonly IWriterMetricsSource _writerMetricsSource;

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
    }

    private static string NextWriterName => $"writer-{Interlocked.Increment(ref _lastWriterId)}";

    internal WriterMetricsReporter(string endpoint, string database, string topic, string? writerName,
        IWriterMetricsSource writerMetricsSource)
    {
        _writerMetricsSource = writerMetricsSource;
        _commonTags =
        [
            new KeyValuePair<string, object?>("endpoint", endpoint),
            new KeyValuePair<string, object?>("database", database),
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("writer.name", writerName ?? NextWriterName)
        ];
        lock (Reporters)
        {
            Reporters.Add(this);
        }
    }

    internal void ReportWritten() => WrittenMessages.Add(1, _commonTags);

    internal void ReportSending() => SendingMessages.Add(1, _commonTags);

    internal void ReportSendingBytes(long bytes) => SendingBytes.Add(bytes, _commonTags);

    internal void ReportSessionError(StatusCode statusCode, bool retry = true) =>
        TopicMetricsUtils.ReportSessionError(SessionErrors, _commonTags, statusCode, retry);

    public void Dispose()
    {
        lock (Reporters)
        {
            Reporters.Remove(this);
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
}
