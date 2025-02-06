using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Services.Topic.Reader;

internal class PartitionSession
{
    private readonly ILogger _logger;
    private readonly ConcurrentQueue<CommitSending> _waitCommitMessages = new();

    private volatile bool _isStopped;

    public PartitionSession(
        ILogger logger,
        long partitionSessionId,
        string topicPath,
        long partitionId,
        long commitedOffset)
    {
        _logger = logger;
        PartitionSessionId = partitionSessionId;
        TopicPath = topicPath;
        PartitionId = partitionId;
        PrevEndOffsetMessage = commitedOffset;
        CommitedOffset = commitedOffset;
    }

    // Identifier of partition session. Unique inside one RPC call.
    internal long PartitionSessionId { get; }

    // Topic path of partition
    internal string TopicPath { get; }

    // Partition identifier
    internal long PartitionId { get; }

    internal long PrevEndOffsetMessage { get; set; }

    // Each offset up to and including (committed_offset - 1) was fully processed.
    internal long CommitedOffset { get; private set; }

    internal void RegisterCommitRequest(CommitSending commitSending)
    {
        var endOffset = commitSending.OffsetsRange.End;

        if (endOffset < CommitedOffset)
        {
            commitSending.TcsCommit.SetResult();
        }
        else
        {
            _waitCommitMessages.Enqueue(commitSending);

            if (_isStopped)
            {
                Utils.SetPartitionClosedException(commitSending, PartitionSessionId);
            }
        }
    }

    internal void HandleCommitedOffset(long commitedOffset)
    {
        if (CommitedOffset >= commitedOffset)
        {
            _logger.LogError(
                "PartitionSession[{PartitionSessionId}] received CommitOffsetResponse[CommitedOffset={CommitedOffset}] " +
                "which is not greater than previous committed offset: {PrevCommitedOffset}",
                PartitionSessionId, commitedOffset, CommitedOffset);
        }

        CommitedOffset = commitedOffset;

        while (_waitCommitMessages.TryPeek(out var waitCommitTcs) &&
               waitCommitTcs.OffsetsRange.End <= commitedOffset)
        {
            _waitCommitMessages.TryDequeue(out _);
            waitCommitTcs.TcsCommit.SetResult();
        }
    }

    internal void Stop()
    {
        _isStopped = true;
        while (_waitCommitMessages.TryDequeue(out var commitSending))
        {
            Utils.SetPartitionClosedException(commitSending, PartitionSessionId);
        }
    }
}
