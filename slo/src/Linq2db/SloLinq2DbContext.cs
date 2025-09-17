using System.Diagnostics;
using System.Security.Cryptography;
using System.Threading.RateLimiting;
using Grpc.Core;
using Internal;
using LinqToDB;
using LinqToDB.Async;
using LinqToDB.Data;
using LinqToDB.Mapping;
using LinqToDB.DataProvider;
using Microsoft.Extensions.Logging;
using Prometheus;
using Ydb.Sdk.Ado;

namespace Linq2db;

/// <summary>
/// SLO harness implemented on top of LINQ to DB provider for YDB.
/// Mirrors behavior of other SLO contexts (ADO.NET/EF/Topic) in this repo.
/// </summary>
public sealed class SloLinq2DbContext : ISloContext
{
    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloLinq2DbContext>();

    // Prometheus metrics (shared labels: operation, status)
    private static readonly Counter Requests = Metrics.CreateCounter(
        "ydb_slo_requests_total",
        "Total number of SLO operations processed.",
        new CounterConfiguration { LabelNames = ["operation", "status"] });

    private static readonly Histogram Duration = Metrics.CreateHistogram(
        "ydb_slo_duration_seconds",
        "Duration of SLO operations.",
        new HistogramConfiguration {
            LabelNames = ["operation", "status"],
            Buckets = Histogram.ExponentialBuckets(start: 0.002, factor: 1.5, count: 20)
        });

    public async Task Create(CreateConfig config)
    {
        Logger.LogInformation("Create: connection={ConnectionString}, initialCount={InitialCount}, writeTimeout={Timeout}s",
            config.ConnectionString, config.InitialDataCount, config.WriteTimeout);

        using var ydb = new YdbConnection(config.ConnectionString);
        await ydb.OpenAsync();

        var provider = ResolveYdbProvider();
        using var db = new DataConnection(provider, ydb);
        db.AddMappingSchema(CreateMapping());

        await EnsureTableAsync(db);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(config.WriteTimeout));
        var now = DateTime.UtcNow;

        const int batchSize = 500;
        int total = config.InitialDataCount;
        int inserted = 0;

        for (int i = 1; i <= total; i += batchSize)
        {
            var take = Math.Min(batchSize, total - i + 1);
            var batch = new List<SloTable>(capacity: take);
            for (int j = 0; j < take; j++)
            {
                var id = i + j;
                batch.Add(new SloTable
                {
                    Guid = MakeGuidFromInt(id),
                    Id = id,
                    PayloadStr = $"seed-{id}",
                    PayloadDouble = id * 1.0,
                    PayloadTimestamp = now
                });
            }

            try
            {
                await db.BulkCopyAsync(new BulkCopyOptions { KeepIdentity = true }, batch, cts.Token);
                inserted += batch.Count;
            }
            catch (NotSupportedException)
            {
                foreach (var e in batch)
                {
                    await db.InsertAsync(e, token: cts.Token);
                    inserted++;
                }
            }
        }

        Logger.LogInformation("Create finished. Seeded: {Inserted} rows.", inserted);
    }

    public async Task Run(RunConfig config)
    {
        Logger.LogInformation(
            "Run: conn={Conn}, pgw={Pgw}, period={Period}ms, readRps={ReadRps}, readTimeout={ReadTimeout}s, writeRps={WriteRps}, writeTimeout={WriteTimeout}s, time={Time}s",
            config.ConnectionString, config.PromPgw, config.ReportPeriod, config.ReadRps, config.ReadTimeout,
            config.WriteRps, config.WriteTimeout, config.Time);

        using var pusher = new MetricPusher(new MetricPusherOptions
        {
            Endpoint = config.PromPgw,
            Job = "ydb_slo_linq2db",
            Instance = Environment.MachineName,
            ReplaceOnPush = true,
            IntervalMilliseconds = config.ReportPeriod
        });
        pusher.Start();

        using var ydb = new YdbConnection(config.ConnectionString);
        await ydb.OpenAsync();

        var provider = ResolveYdbProvider();
        using var db = new DataConnection(provider, ydb);
        db.AddMappingSchema(CreateMapping());

        // Get current max Id
        var maxId = await db.GetTable<SloTable>().Select(t => (int?)t.Id).MaxAsync() ?? 0;
        var nextWriteId = maxId;

        var readLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = Math.Max(1, config.ReadRps),
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            QueueLimit = 0,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            TokensPerPeriod = Math.Max(1, config.ReadRps),
            AutoReplenishment = true
        });

        var writeLimiter = new TokenBucketRateLimiter(new TokenBucketRateLimiterOptions
        {
            TokenLimit = Math.Max(1, config.WriteRps),
            QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
            QueueLimit = 0,
            ReplenishmentPeriod = TimeSpan.FromSeconds(1),
            TokensPerPeriod = Math.Max(1, config.WriteRps),
            AutoReplenishment = true
        });

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(config.Time));

        var readTask = Task.Run(() => LoopAsync("read", ReadOnceAsync), cts.Token);
        var writeTask = Task.Run(() => LoopAsync("write", WriteOnceAsync), cts.Token);

        try
        {
            await Task.WhenAll(readTask, writeTask);
        }
        catch (Exception ex)
        {
            Logger.LogInformation(ex, "Run finished with cancellation or error.");
        }

        pusher.Stop();
        Logger.LogInformation("Run task is finished.");

        return;

        async Task LoopAsync(string operation, Func<CancellationToken, Task> action)
        {
            var limiter = operation == "read" ? readLimiter : writeLimiter;
            var timeout = TimeSpan.FromSeconds(operation == "read" ? config.ReadTimeout : config.WriteTimeout);

            while (!cts.IsCancellationRequested)
            {
                using var lease = await limiter.AcquireAsync(permitCount: 1, cancellationToken: cts.Token);
                if (!lease.IsAcquired) continue;

                using var rpcCts = new CancellationTokenSource(timeout);
                var sw = Stopwatch.StartNew();
                string status = "OK";

                try
                {
                    await action(rpcCts.Token);
                }
                catch (RpcException rpcEx)
                {
                    status = $"GRPC_{rpcEx.Status.StatusCode}";
                    Logger.LogWarning(rpcEx, "GRPC error in {Operation}", operation);
                }
                catch (Exception ex) when (TryExtractStatusLabel(ex, out var statusLabel))
                {
                    status = statusLabel;
                    Logger.LogWarning(ex, "Provider error in {Operation}", operation);
                }
                catch (Exception ex)
                {
                    status = "EXCEPTION";
                    Logger.LogWarning(ex, "Unhandled error in {Operation}", operation);
                }
                finally
                {
                    sw.Stop();
                    Requests.WithLabels(operation, status).Inc();
                    Duration.WithLabels(operation, status).Observe(sw.Elapsed.TotalSeconds);
                }
            }
        }

        async Task ReadOnceAsync(CancellationToken token)
        {
            var currentMax = Math.Max(1, Volatile.Read(ref nextWriteId));
            var id = Random.Shared.Next(1, currentMax + 1);
            var guid = MakeGuidFromInt(id);

            _ = await db.GetTable<SloTable>()
                .Where(t => t.Guid == guid && t.Id == id)
                .FirstOrDefaultAsync(token);
        }

        async Task WriteOnceAsync(CancellationToken token)
        {
            var id = Interlocked.Increment(ref nextWriteId);
            var entity = new SloTable
            {
                Guid = MakeGuidFromInt(id),
                Id = id,
                PayloadStr = $"write-{id}",
                PayloadDouble = id * 1.0,
                PayloadTimestamp = DateTime.UtcNow
            };

            await db.InsertAsync(entity, token: token);
        }
    }

    private static MappingSchema CreateMapping()
    {
        var ms = new MappingSchema();
        var fb = new FluentMappingBuilder(ms);

        fb.Entity<SloTable>()
            .HasTableName(SloTable.Name)
            .Property(e => e.Guid).IsPrimaryKey().IsNullable(false)
            .Property(e => e.Id).IsPrimaryKey().IsNullable(false)
            .Property(e => e.PayloadStr).IsNullable(false)
            .Property(e => e.PayloadDouble).IsNullable(false)
            .Property(e => e.PayloadTimestamp).IsNullable(false);

        return ms;
    }

    private static async Task EnsureTableAsync(DataConnection db)
    {
        try { await db.ExecuteAsync($"DROP TABLE {SloTable.Name};"); } catch { /* ignore */ }

        var create = $@"
CREATE TABLE {SloTable.Name} (
  Guid Uuid,
  Id Int32,
  PayloadStr Utf8,
  PayloadDouble Double,
  PayloadTimestamp Timestamp,
  PRIMARY KEY (Guid, Id)
);";

        await db.ExecuteAsync(create);

        foreach (var stmt in Internal.SloTable.Options.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
            await db.ExecuteAsync(stmt + ";");
    }

    private static Guid MakeGuidFromInt(int id)
    {
        Span<byte> intBytes = stackalloc byte[4];
        BitConverter.TryWriteBytes(intBytes, id);
        var hash = SHA1.HashData(intBytes);
        Span<byte> guidBytes = stackalloc byte[16];
        hash.AsSpan(0,16).CopyTo(guidBytes);
        return new Guid(guidBytes);
    }
    
    private static bool TryExtractStatusLabel(Exception ex, out string label)
    {
        label = "";
        for (var e = ex; e != null; e = e.InnerException!)
        {
            var prop = e.GetType().GetProperty("StatusCode");
            if (prop != null && prop.PropertyType.IsEnum)
            {
                var val = prop.GetValue(e);
                var typeName = prop.PropertyType.FullName ?? prop.PropertyType.Name;
                if (typeName.Contains("Ydb", StringComparison.OrdinalIgnoreCase))
                {
                    label = $"YDB_{val}";
                    return true;
                }
                if (typeName.Contains("Grpc", StringComparison.OrdinalIgnoreCase))
                {
                    label = $"GRPC_{val}";
                    return true;
                }
                label = $"STATUS_{val}";
                return true;
            }
        }
        return false;
    }

    private static IDataProvider ResolveYdbProvider()
    {
        var asms = AppDomain.CurrentDomain.GetAssemblies();
        foreach (var asm in asms)
        {
            foreach (var t in asm.GetTypes())
            {
                if (typeof(IDataProvider).IsAssignableFrom(t) && !t.IsAbstract && !t.IsInterface)
                {
                    var name = t.FullName ?? t.Name;
                    if (name.Contains("Ydb", StringComparison.OrdinalIgnoreCase) ||
                        name.Contains("YDB", StringComparison.OrdinalIgnoreCase))
                    {
                        return (IDataProvider)Activator.CreateInstance(t)!;
                    }
                }
            }
        }
        throw new InvalidOperationException("YDB IDataProvider not found. Ensure your Linq2DB YDB provider assembly is referenced.");
    }
}
