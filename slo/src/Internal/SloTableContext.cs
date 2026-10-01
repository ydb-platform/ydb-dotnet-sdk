using System.Threading.RateLimiting;
using Microsoft.Extensions.Logging;
using NLog.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Ydb.Sdk;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace Internal;

public interface ISloContext
{
    public static readonly ILoggerFactory Factory = LoggerFactory.Create(builder => builder.AddNLog());
    public Task Run(SloConfig sloConfig);
}

public abstract class SloTableContext<T> : ISloContext
{
    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloTableContext<T>>();
    private readonly List<SloTable> _confirmed = [];
    private readonly object _ledgerLock = new();
    private readonly Guid _namespace = Guid.NewGuid();
    private int _nextId;
    private long _reads;
    private long _readErrors;
    private long _writes;
    private long _writeErrors;

    protected abstract string Job { get; }
    protected abstract T CreateClient(SloConfig config);
    protected abstract Task Create(T client, int operationTimeout);
    protected abstract Task<int> Save(T client, SloTable row, int writeTimeout);
    protected abstract Task<SloTable?> Select(T client, (Guid Guid, int Id) key, int readTimeout);
    protected abstract Task<int> SelectCount(T client);

    public async Task Run(SloConfig config)
    {
        var runId = Guid.NewGuid().ToString("N");
        var checks = new List<SloCheck>();
        using var lifetime = new CancellationTokenSource();
        try
        {
            if (config.CompletionTimeout <= 0)
                throw new ArgumentException("SLO completion timeout must be positive");
            lifetime.CancelAfter(TimeSpan.FromSeconds(config.CompletionTimeout));
            checks = await Task.Run(() => RunWorkload(config, runId, lifetime.Token))
                .WaitAsync(lifetime.Token);
        }
        catch (Exception error)
        {
            var detail = lifetime.IsCancellationRequested
                ? $"Completion deadline expired after {config.CompletionTimeout}s"
                : error.Message;
            checks.Add(new SloCheck("T01", "FAIL", detail));
            checks.Add(new SloCheck("T02", "FAIL", detail));
            throw;
        }
        finally
        {
            await SloResult.WriteAsync(runId, "table", checks,
            [
                new SloOperationCount("read", Interlocked.Read(ref _reads), Interlocked.Read(ref _readErrors)),
                new SloOperationCount("write", Interlocked.Read(ref _writes), Interlocked.Read(ref _writeErrors))
            ]);
        }
    }

    private async Task<List<SloCheck>> RunWorkload(SloConfig config, string runId, CancellationToken lifetime)
    {
        using var telemetry = SloTelemetry.Create(config, Job, runId);
        var client = CreateClient(config);
        var checks = new List<SloCheck>();
        try
        {
            if (config.ReadRps <= 0 || config.WriteRps <= 0 || config.Time <= 0
                || config.InitialDataCount <= 0 || config.ReadTimeout <= 0 || config.WriteTimeout <= 0)
                throw new ArgumentException("SLO requires positive duration, read/write RPS and seed count");

            using (var initialization = new CancellationTokenSource(TimeSpan.FromSeconds(config.WriteTimeout)))
            {
                for (var attempt = 1; ; attempt++)
                {
                    try
                    {
                        await Create(client, config.WriteTimeout);
                        break;
                    }
                    catch (YdbException error)
                    {
                        var delay = YdbRetryPolicy.IdempotenceDefault.GetNextDelay(error, attempt);
                        if (delay is null) throw;
                        await Task.Delay(delay.Value, initialization.Token);
                    }
                }
            }
            for (var i = 0; i < config.InitialDataCount; i++)
            {
                for (var attempt = 1; ; attempt++)
                {
                    try
                    {
                        await Write(client, config);
                        break;
                    }
                    catch (YdbException error)
                    {
                        Interlocked.Increment(ref _writeErrors);
                        var delay = YdbRetryPolicy.IdempotenceDefault.GetNextDelay(error, attempt);
                        if (delay is null) throw;
                        Logger.LogWarning(error, "Initial write failed; retrying within completion budget");
                        await Task.Delay(delay.Value, lifetime);
                    }
                }
            }
            using var duration = CancellationTokenSource.CreateLinkedTokenSource(lifetime);
            duration.CancelAfter(TimeSpan.FromSeconds(config.Time));
            await Task.WhenAll(Shoot(client, config, false, duration),
                Shoot(client, config, true, duration));

            SloTable[] rows;
            lock (_ledgerLock) rows = _confirmed.ToArray();
            using var verification = new CancellationTokenSource(TimeSpan.FromSeconds(config.Time + config.ReadTimeout));
            using var verificationRate = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
            {
                TokenLimit = config.ReadRps, TokensPerPeriod = config.ReadRps,
                ReplenishmentPeriod = TimeSpan.FromSeconds(1), AutoReplenishment = true,
                QueueLimit = 10, QueueProcessingOrder = QueueProcessingOrder.OldestFirst
            });
            await Parallel.ForEachAsync(rows, new ParallelOptions
            {
                MaxDegreeOfParallelism = 10, CancellationToken = verification.Token
            }, async (row, token) =>
            {
                using var lease = await verificationRate.AcquireAsync(cancellationToken: token);
                if (!lease.IsAcquired) throw new InvalidOperationException("Final verification rate lease denied");
                var actual = await Select(client, (row.Guid, row.Id), config.ReadTimeout)
                    .WaitAsync(token);
                Verify(row, actual);
            });
            checks.Add(new SloCheck("T01", "PASS", $"Verified {rows.Length} confirmed writes"));
            checks.Add(new SloCheck("T02", _reads > 0 ? "PASS" : "INVALID",
                $"Verified reads: {_reads}; operation errors: {_readErrors}"));
        }
        finally
        {
            if (client is IAsyncDisposable asyncDisposable) await asyncDisposable.DisposeAsync();
            else if (client is IDisposable disposable) disposable.Dispose();
            telemetry?.ForceFlush();
        }
        return checks;
    }

    private async Task Write(T client, SloConfig config)
    {
        var now = DateTime.UtcNow;
        var id = Interlocked.Increment(ref _nextId);
        var row = new SloTable
        {
            Guid = _namespace,
            Id = id,
            PayloadStr = $"{_namespace:N}:{id}",
            PayloadDouble = Random.Shared.NextDouble(),
            PayloadTimestamp = new DateTime(now.Ticks - now.Ticks % 10, DateTimeKind.Utc)
        };
        await Save(client, row, config.WriteTimeout);
        lock (_ledgerLock) _confirmed.Add(row);
        Interlocked.Increment(ref _writes);
    }

    private async Task Shoot(T client, SloConfig config, bool read, CancellationTokenSource stop)
    {
        var token = stop.Token;
        using var limiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = read ? config.ReadRps : config.WriteRps,
            TokensPerPeriod = read ? config.ReadRps : config.WriteRps,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            AutoReplenishment = true,
            QueueLimit = 10,
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst
        });
        await Task.WhenAll(Enumerable.Range(0, 10).Select(async _ =>
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    using var lease = await limiter.AcquireAsync(cancellationToken: token);
                    if (!lease.IsAcquired) continue;
                    if (!read)
                    {
                        await Write(client, config);
                        continue;
                    }
                    SloTable row;
                    lock (_ledgerLock) row = _confirmed[Random.Shared.Next(_confirmed.Count)];
                    var actual = await Select(client, (row.Guid, row.Id), config.ReadTimeout);
                    Verify(row, actual);
                    Interlocked.Increment(ref _reads);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    break;
                }
                catch (YdbException error)
                {
                    if (read) Interlocked.Increment(ref _readErrors);
                    else Interlocked.Increment(ref _writeErrors);
                    Logger.LogWarning(error, "Logical {Operation} failed", read ? "read" : "write");
                }
                catch
                {
                    if (read) Interlocked.Increment(ref _readErrors);
                    else Interlocked.Increment(ref _writeErrors);
                    await stop.CancelAsync();
                    throw;
                }
            }
        }));
    }

    private static void Verify(SloTable expected, SloTable? actual)
    {
        if (actual is null || expected.Guid != actual.Guid || expected.Id != actual.Id
            || expected.PayloadStr != actual.PayloadStr || expected.PayloadDouble != actual.PayloadDouble
            || expected.PayloadTimestamp.Ticks != actual.PayloadTimestamp.Ticks)
            throw new InvalidDataException($"Confirmed row payload mismatch: {expected.Guid}/{expected.Id}");
    }
}

public static class StatusCodeExtension
{
    public static string StatusName(this StatusCode statusCode)
    {
        var prefix = statusCode >= StatusCode.ClientTransportResourceExhausted ? "GRPC" : "YDB";
        return $"{prefix}_{statusCode}";
    }
}
