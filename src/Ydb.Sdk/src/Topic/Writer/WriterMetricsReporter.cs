using System.Diagnostics;
using System.Diagnostics.Metrics;
using Ydb.Sdk.Internal;

namespace Ydb.Sdk.Topic.Writer;

internal sealed class WriterMetricsReporter
{
    private static long _lastWriterId;

    private static readonly Counter<long> WrittenMessages;

    private readonly KeyValuePair<string, object?>[] _commonTags;

    static WriterMetricsReporter()
    {
        var meter = new Meter("Ydb.Sdk.Topic", YdbSdkVersion.Value);

        WrittenMessages = meter.CreateCounter<long>(
            "ydb.topic.writer.written.messages",
            unit: "{message}",
            description: "The number of messages confirmed written by the server, including already written messages.");
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

    internal void ReportWritten(PersistenceStatus status)
    {
        if (!WrittenMessages.Enabled)
        {
            return;
        }

        WrittenMessages.Add(1, new TagList(_commonTags)
        {
            { "status", status == PersistenceStatus.Written ? "written" : "already_written" }
        });
    }
}
