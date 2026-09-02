using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Topic.Reader;

internal class PartitionSession(
    ILogger logger,
    long partitionSessionId,
    string topicPath,
    long partitionId,
    long commitedOffset)
{
    private readonly ConcurrentQueue<CommitSending> _waitCommitMessages = new();

    private volatile bool _isStopped;

    internal bool IsActive => !_isStopped;

    // Identifier of partition session. Unique inside one RPC call.
    internal long PartitionSessionId => partitionSessionId;

    // Topic path of partition
    internal string TopicPath => topicPath;

    // Partition identifier
    internal long PartitionId => partitionId;

    internal long PrevEndOffsetMessage { get; set; } = commitedOffset;

    // Each offset up to and including (committed_offset - 1) was fully processed.
    private long CommitedOffset { get; set; } = commitedOffset;

    internal void RegisterCommitRequest(CommitSending commitSending)
    {
        var endOffset = commitSending.OffsetsRange.End;

        if (endOffset < CommitedOffset)
        {
            commitSending.TcsCommit.SetResult();
        }
        else
        {
            if (_isStopped)
            {
                Utils.SetPartitionClosedException(commitSending, PartitionSessionId);

                return;
            }

            _waitCommitMessages.Enqueue(commitSending);
        }
    }

    internal long HandleCommitedOffset(long commitedOffset)
    {
        if (CommitedOffset >= commitedOffset)
        {
            logger.LogError(
                "PartitionSession[{PartitionSessionId}] received CommitOffsetResponse[CommitedOffset={CommitedOffset}] " +
                "which is not greater than previous committed offset: {PrevCommitedOffset}",
                PartitionSessionId, commitedOffset, CommitedOffset);
        }

        CommitedOffset = commitedOffset;
        var acknowledgedMessages = 0L;

        while (_waitCommitMessages.TryPeek(out var waitCommitTcs) &&
               waitCommitTcs.OffsetsRange.End <= commitedOffset)
        {
            _waitCommitMessages.TryDequeue(out _);
            waitCommitTcs.TcsCommit.SetResult();
            acknowledgedMessages += waitCommitTcs.OffsetsRange.End - waitCommitTcs.OffsetsRange.Start;
        }

        return acknowledgedMessages;
    }

    internal void Stop(long commitedOffset)
    {
        _isStopped = true;

        while (_waitCommitMessages.TryDequeue(out var commitSending))
        {
            if (commitSending.OffsetsRange.End <= commitedOffset)
            {
                commitSending.TcsCommit.SetResult();
            }
            else
            {
                Utils.SetPartitionClosedException(commitSending, PartitionSessionId);
            }
        }
    }
}
