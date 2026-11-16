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
    private long _commitedOffset = commitedOffset;

    internal bool IsActive => !_isStopped;

    // Identifier of partition session. Unique inside one RPC call.
    internal long PartitionSessionId => partitionSessionId;

    // Topic path of partition
    internal string TopicPath => topicPath;

    // Partition identifier
    internal long PartitionId => partitionId;

    internal long PrevEndOffsetMessage { get; set; } = commitedOffset;

    internal long CommitOffsetLag =>
        Math.Max(0, _waitCommitMessages.LastOrDefault()?.OffsetsRange.End - CommitedOffset ?? 0);

    // Each offset up to and including (committed_offset - 1) was fully processed.
    private long CommitedOffset
    {
        get => Volatile.Read(ref _commitedOffset);
        set => Volatile.Write(ref _commitedOffset, value);
    }

    internal bool RegisterCommitRequest(CommitSending commitSending)
    {
        var endOffset = commitSending.OffsetsRange.End;

        if (endOffset < CommitedOffset)
        {
            commitSending.TcsCommit.SetResult();

            return false;
        }

        // Enqueue before checking _isStopped: if Stop drains the queue in between, either Stop sees this commit
        // in the queue or this check sees _isStopped, so the commit is always completed.
        _waitCommitMessages.Enqueue(commitSending);

        if (_isStopped)
        {
            Utils.SetPartitionClosedException(commitSending, PartitionSessionId);

            return false;
        }

        return true;
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
        // Keeps the _isStopped write from being reordered after the queue reads below (RegisterCommitRequest
        // relies on it). Enqueue publishes the commit with an interlocked operation, a full fence on its side.
        Interlocked.MemoryBarrier();

        while (_waitCommitMessages.TryDequeue(out var commitSending))
        {
            // RegisterCommitRequest may have already failed this commit after seeing _isStopped
            if (commitSending.OffsetsRange.End <= commitedOffset)
            {
                commitSending.TcsCommit.TrySetResult();
            }
            else
            {
                Utils.SetPartitionClosedException(commitSending, PartitionSessionId);
            }
        }
    }
}
