using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Writer;

internal sealed class WriterMetricsReporter
{
    private static long _lastWriterId;

    private static readonly Counter<long> WrittenMessages;
    private static readonly Counter<long> SendingMessages;
    private static readonly Counter<long> SessionErrors;

    private readonly KeyValuePair<string, object?>[] _commonTags;

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
        SessionErrors = meter.CreateCounter<long>(
            "ydb.topic.writer.session.errors",
            unit: "{error}",
            description: "The number of writer stream session errors by retry decision.");
    }

    private static string NextWriterName => $"writer-{Interlocked.Increment(ref _lastWriterId)}";

    internal WriterMetricsReporter(string endpoint, string database, string topic, string? writerName)
    {
        _commonTags =
        [
            new KeyValuePair<string, object?>("endpoint", endpoint),
            new KeyValuePair<string, object?>("database", database),
            new KeyValuePair<string, object?>("topic", topic),
            new KeyValuePair<string, object?>("writer.name", writerName ?? NextWriterName)
        ];
    }

    internal void ReportWritten() => WrittenMessages.Add(1, _commonTags);

    internal void ReportSending() => SendingMessages.Add(1, _commonTags);

    internal void ReportSessionError(StatusCode statusCode, bool retry = true) =>
        MetricUtils.ReportSessionError(SessionErrors, _commonTags, statusCode, retry);
}
