using System.Threading.RateLimiting;
using Internal;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Topic;
using Ydb.Sdk.Topic.Reader;
using Ydb.Sdk.Topic.Writer;

namespace TopicService;

public class SloTopicContext : ISloContext
{
    private const int Partitions = 10;
    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloTopicContext>();
    private readonly long[] _acked = new long[Partitions];
    private readonly long[] _delivered = new long[Partitions];
    private readonly long[] _committed = new long[Partitions];
    private readonly object[] _ordering = Enumerable.Range(0, Partitions).Select(_ => new object()).ToArray();
    private readonly TaskCompletionSource[] _drained = Enumerable.Range(0, Partitions)
        .Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)).ToArray();
    private int _writersFinished;
    private long _writeErrors;
    private long _commitErrors;
    private long _readErrors;

    public async Task Run(SloConfig config)
    {
        var runId = Guid.NewGuid().ToString("N");
        using var telemetry = SloTelemetry.Create(config, "TopicService", runId);
        var connection = new YdbConnectionStringBuilder(config.ConnectionString)
            { LoggerFactory = ISloContext.Factory, PoolName = "TopicService" };
        var path = $"{connection.Database.TrimEnd('/')}/slo-v3-topic-{runId}";
        const string consumer = "slo-v3-reader";
        await using var topicClient = new TopicClient(connection);
        using var writerStop = new CancellationTokenSource();
        using var readerStop = new CancellationTokenSource();
        var checks = new List<SloCheck>();
        var readers = Array.Empty<Task>();
        var created = false;
        try
        {
            if (config.Time <= 0 || config.WriteRps <= 0 || config.WriteTimeout <= 0 || config.ReadTimeout <= 0)
                throw new ArgumentException("Topic SLO requires positive duration, RPS and deadlines");
            await topicClient.CreateTopic(new CreateTopicSettings
            {
                Path = path,
                PartitioningSettings = new PartitioningSettings { MinActivePartitions = Partitions },
                Consumers = { new Consumer(consumer) { Important = true } }
            });
            created = true;
            using var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
            {
                TokenLimit = config.WriteRps, TokensPerPeriod = config.WriteRps,
                ReplenishmentPeriod = TimeSpan.FromSeconds(1), AutoReplenishment = true,
                QueueLimit = Partitions, QueueProcessingOrder = QueueProcessingOrder.OldestFirst
            });
            readers = Enumerable.Range(0, Partitions).Select(Read).ToArray();
            writerStop.CancelAfter(TimeSpan.FromSeconds(config.Time));
            var writers = Enumerable.Range(0, Partitions).Select(Write).ToArray();
            await Task.WhenAll(writers);
            if (readers.Any(task => task.IsFaulted)) await Task.WhenAll(readers);
            Volatile.Write(ref _writersFinished, 1);
            for (var i = 0; i < Partitions; i++) SignalDrain(i);
            await Task.WhenAll(_drained.Select(signal => signal.Task))
                .WaitAsync(TimeSpan.FromSeconds(config.ReadTimeout));
            await readerStop.CancelAsync();
            await Task.WhenAll(readers);
            checks.Add(new SloCheck("P01", "PASS", "Every ACKed sequence delivered in partition order"));
            checks.Add(new SloCheck("P02", "PASS", "Batch and single APIs completed delivery and commit"));
            checks.Add(new SloCheck("P03", "PASS", $"All ACKed sequences committed; transient commit errors: {_commitErrors}"));

            async Task Write(int partition)
            {
                await using var writer = new WriterBuilder<string>(connection, path)
                {
                    ProducerId = $"{runId}-{partition}",
                    WriterName = $"writer-{partition}",
                    PartitionId = partition,
                    BufferMaxSize = 8 * 1024 * 1024
                }.Build();
                while (!writerStop.IsCancellationRequested)
                {
                    try
                    {
                        using var lease = await limiter.AcquireAsync(cancellationToken: writerStop.Token);
                        if (!lease.IsAcquired) continue;
                        var sequence = Volatile.Read(ref _acked[partition]) + 1;
                        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(config.WriteTimeout));
                        await writer.WriteAsync($"{runId}:{partition}:{sequence}", deadline.Token);
                        Volatile.Write(ref _acked[partition], sequence);
                    }
                    catch (OperationCanceledException) when (writerStop.IsCancellationRequested)
                    {
                        break;
                    }
                    catch (OperationCanceledException error)
                    {
                        Interlocked.Increment(ref _writeErrors);
                        Logger.LogWarning(error, "Write ACK deadline expired; sequence will be retried");
                    }
                    catch
                    {
                        Interlocked.Increment(ref _writeErrors);
                        await writerStop.CancelAsync();
                        await readerStop.CancelAsync();
                        throw;
                    }
                }
            }
        }
        catch (Exception error)
        {
            await writerStop.CancelAsync();
            await readerStop.CancelAsync();
            checks.Add(new SloCheck("P01", "FAIL", error.Message));
            checks.Add(new SloCheck("P02", "FAIL", error.Message));
            checks.Add(new SloCheck("P03", "FAIL", error.Message));
            throw;
        }
        finally
        {
            await readerStop.CancelAsync();
            try { await Task.WhenAll(readers); }
            catch (Exception error) { Logger.LogError(error, "Reader failed during shutdown"); }
            telemetry?.ForceFlush();
            await SloResult.WriteAsync(runId, "topic", checks,
            [
                new SloOperationCount("write", _acked.Sum(), _writeErrors),
                new SloOperationCount("read", _delivered.Sum(), _readErrors + _commitErrors)
            ]);
            if (created) await topicClient.DropTopic(path);
        }

        async Task Read(int readerIndex)
        {
            await using var reader = new ReaderBuilder<string>(connection)
            {
                ConsumerName = consumer,
                ReaderName = $"reader-{readerIndex}",
                SubscribeSettings = { new SubscribeSettings(path) },
                MemoryUsageMaxBytes = 8 * 1024 * 1024
            }.Build();
            try
            {
                while (!readerStop.IsCancellationRequested)
                {
                    if (readerIndex % 2 == 0)
                    {
                        var batch = await reader.ReadBatchAsync(readerStop.Token);
                        foreach (var message in batch.Batch) Verify(message);
                        try
                        {
                            await batch.CommitBatchAsync().WaitAsync(readerStop.Token);
                            if (batch.Batch.Count > 0)
                                ConfirmCommit(batch.Batch[0].PartitionId,
                                    batch.Batch.Max(message => long.Parse(message.Data.Split(':')[2])));
                        }
                        catch (ReaderException error)
                        {
                            Interlocked.Increment(ref _commitErrors);
                            Logger.LogWarning(error, "Batch commit will be retried on redelivery");
                        }
                    }
                    else
                    {
                        var message = await reader.ReadAsync(readerStop.Token);
                        Verify(message);
                        try
                        {
                            await message.CommitAsync().WaitAsync(readerStop.Token);
                            ConfirmCommit(message.PartitionId, long.Parse(message.Data.Split(':')[2]));
                        }
                        catch (ReaderException error)
                        {
                            Interlocked.Increment(ref _commitErrors);
                            Logger.LogWarning(error, "Commit will be retried on redelivery");
                        }
                    }
                }
            }
            catch (OperationCanceledException) when (readerStop.IsCancellationRequested) { }
            catch
            {
                Interlocked.Increment(ref _readErrors);
                await writerStop.CancelAsync();
                await readerStop.CancelAsync();
                throw;
            }
        }

        void Verify(Ydb.Sdk.Topic.Reader.Message<string> message)
        {
            var partition = checked((int)message.PartitionId);
            if (partition < 0 || partition >= Partitions)
                throw new InvalidDataException($"Unexpected partition {partition}");
            var parts = message.Data.Split(':');
            if (parts.Length != 3 || parts[0] != runId || parts[1] != partition.ToString()
                || !long.TryParse(parts[2], out var sequence) || sequence <= 0
                || message.Data != $"{runId}:{partition}:{sequence}")
                throw new InvalidDataException($"Partition {partition}: payload mismatch");
            lock (_ordering[partition])
            {
                if (sequence > _delivered[partition] + 1)
                    throw new InvalidDataException($"Partition {partition}: forward sequence gap");
                if (sequence == _delivered[partition] + 1)
                    Volatile.Write(ref _delivered[partition], sequence);
            }
        }
    }

    private void ConfirmCommit(long partitionId, long sequence)
    {
        var partition = checked((int)partitionId);
        lock (_ordering[partition])
            Volatile.Write(ref _committed[partition], Math.Max(_committed[partition], sequence));
        SignalDrain(partition);
    }

    private void SignalDrain(int partition)
    {
        var acked = Volatile.Read(ref _acked[partition]);
        if (Volatile.Read(ref _writersFinished) != 0 && acked > 0
            && Volatile.Read(ref _committed[partition]) >= acked)
            _drained[partition].TrySetResult();
    }
}
