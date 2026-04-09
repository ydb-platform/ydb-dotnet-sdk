using System.Runtime.CompilerServices;
using System.Threading.RateLimiting;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using Ydb.Sdk.Ado;
using Ydb.Sdk.OpenTelemetry;

const string ydbConnectionString = "Host=ydb;Port=2136;Database=/local";

// ── Load-tank mode ────────────────────────────────────────────────────────
// Runs a rising / falling RPS pattern so that pool session-count metrics
// (db.client.connection.count) visibly change over time in Grafana.
//
// Pattern per cycle (repeating):
//   Peak   (200 RPS, 60 s) → Medium (40 RPS, 90 s) → Min (4 RPS, 60 s)
//                          → Medium (40 RPS, 90 s) → [repeat]
//
// MinPoolSize=0 + short SessionIdleTimeout lets the pool shrink during
// the low-RPS phase so the connection-count gauge visibly falls.

const int peakRps = 200;
const int mediumRps = 40;
const int minRps = 4;
const int peakDurationSec = 600;
const int mediumDurationSec = 900;
const int minDurationSec = 600;
var totalSec = args.Length > 1 && int.TryParse(args[1], out var s) ? s : 3000;
const int workerCount = 60;

await using var dataSource = new YdbDataSource(
    new YdbConnectionStringBuilder(ydbConnectionString)
    {
        MaxPoolSize = workerCount,
        MinPoolSize = 0,
        SessionIdleTimeout = 15,
        PoolName = "load-tank"
    });

using var loadMeterProvider = Sdk.CreateMeterProviderBuilder()
    .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService("ydb-load-tank"))
    .AddYdb()
    .AddMeter("System.Net.Http")
    .AddOtlpExporter((exporterOptions, metricReaderOptions) =>
    {
        exporterOptions.Endpoint = new Uri("http://otel-collector:4317");
        metricReaderOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 2000;
    })
    .Build();

using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(totalSec));
var token = cts.Token;

Console.WriteLine(
    $"=== YDB Metrics Load Tank ({totalSec / 60}m, {workerCount} workers) ===\n" +
    $"    Pattern: Peak({peakRps} rps, {peakDurationSec}s) " +
    $"→ Medium({mediumRps} rps, {mediumDurationSec}s) " +
    $"→ Min({minRps} rps, {minDurationSec}s) → Medium → repeat");

var currentRpsCts = new CancellationTokenSource();

// RPS controller: iterates the load pattern and restarts workers on each step
var controller = Task.Run(async () =>
{
    await foreach (var (rps, label) in LoadStepsAsync(
                       peakRps, peakDurationSec,
                       mediumRps, mediumDurationSec,
                       minRps, minDurationSec,
                       totalSec, token))
    {
        await currentRpsCts.CancelAsync();
        currentRpsCts = new CancellationTokenSource();
        var stepToken = CancellationTokenSource.CreateLinkedTokenSource(token, currentRpsCts.Token).Token;

        Console.WriteLine($"[{DateTime.Now:HH:mm:ss}] Phase: {label} ({rps} RPS)");
        // ReSharper disable once AccessToDisposedClosure
        _ = RunWorkersAsync(dataSource, rps, workerCount, stepToken);
    }
}, token);

try
{
    await controller;
}
catch (OperationCanceledException)
{
    /* expected */
}
finally
{
    await currentRpsCts.CancelAsync();
}

Console.WriteLine("Load tank finished.");

// ── Demo mode ────────────────────────────────────────────────────────────────
// Phase 1: N workers continuously insert rows (happy-path operation metrics).
// Phase 2: a few intentional errors to populate db.client.operation.failed.

await using var ds = new YdbDataSource(ydbConnectionString);

Console.WriteLine("Initializing...");

await ds.ExecuteAsync(async conn =>
{
    await new YdbCommand("DROP TABLE IF EXISTS demo_inserts", conn).ExecuteNonQueryAsync();
    await new YdbCommand("CREATE TABLE IF NOT EXISTS demo_inserts(id Uuid, val Int32, PRIMARY KEY (id))", conn)
        .ExecuteNonQueryAsync();
    await new YdbCommand("CREATE TABLE IF NOT EXISTS bank(id Int32, amount Int32, PRIMARY KEY (id))", conn)
        .ExecuteNonQueryAsync();
    await new YdbCommand("INSERT INTO bank(id, amount) VALUES (2, 0)", conn).ExecuteNonQueryAsync();
});

Console.WriteLine("Phase 1: inserting rows (Ctrl+C to stop)...");

using var demoCts = new CancellationTokenSource();
demoCts.CancelAfter(TimeSpan.FromSeconds(30));

var demoToken = demoCts.Token;
const int demoWorkers = 10;

var insertTasks = Enumerable.Range(0, demoWorkers).Select(i => Task.Run(async () =>
{
    // ReSharper disable once AccessToDisposedClosure
    await using var conn = await ds.OpenConnectionAsync();
    while (!demoToken.IsCancellationRequested)
    {
        try
        {
            await new YdbCommand("INSERT INTO demo_inserts(id, val) VALUES (RandomUuid(0), @v)", conn)
            {
                Parameters = { new YdbParameter { Value = i, ParameterName = "v" } }
            }.ExecuteNonQueryAsync(demoToken);
        }
        catch (OperationCanceledException)
        {
            break;
        }
        catch
        {
            /* ignored */
        }
    }
}));

await Task.WhenAll(insertTasks);

// Phase 2 — error pass: query a non-existent table to populate db.client.operation.failed
Console.WriteLine("Phase 2: generating errors...");
await using var errConn = await ds.OpenConnectionAsync();
for (var i = 0; i < 100; i++)
{
    try
    {
        await new YdbCommand("SELECT * FROM table_that_does_not_exist_xyz", errConn).ExecuteNonQueryAsync();
    }
    catch
    {
        /* expected */
    }
}

var tasks = new List<Task>();
for (var i = 0; i < 10; i++)
{
    tasks.Add(Task.Run(async () =>
    {
        // ReSharper disable once AccessToDisposedClosure
        await ds.ExecuteInTransactionAsync(async ydbConnection =>
        {
            var count = (int)(await new YdbCommand(ydbConnection)
                { CommandText = "SELECT amount FROM bank WHERE id = 2" }.ExecuteScalarAsync())!;

            await new YdbCommand(ydbConnection)
            {
                CommandText = "UPDATE bank SET amount = @amount + 1 WHERE id = 2",
                Parameters = { new YdbParameter { Value = count, ParameterName = "amount" } }
            }.ExecuteNonQueryAsync();
        });
    }));
}

await Task.WhenAll(tasks);

Console.WriteLine("Waiting 15s to flush remaining metrics...");
await Task.Delay(15000);
return;

// ── Helpers ──────────────────────────────────────────────────────────────────

static async IAsyncEnumerable<(int Rps, string Label)> LoadStepsAsync(
    int peakRps, int peakSec,
    int mediumRps, int mediumSec,
    int minRps, int minSec,
    int totalSec,
    [EnumeratorCancellation] CancellationToken cancellationToken = default)
{
    var elapsed = 0;
    var totalMs = totalSec * 1000;

    while (elapsed < totalMs && !cancellationToken.IsCancellationRequested)
    {
        yield return (peakRps, "Peak");
        var d = Math.Min(peakSec * 1000, totalMs - elapsed);
        await Task.Delay(d, cancellationToken);
        elapsed += d;
        if (elapsed >= totalMs) break;

        yield return (mediumRps, "Medium↓");
        d = Math.Min(mediumSec * 1000, totalMs - elapsed);
        await Task.Delay(d, cancellationToken);
        elapsed += d;
        if (elapsed >= totalMs) break;

        yield return (minRps, "Min");
        d = Math.Min(minSec * 1000, totalMs - elapsed);
        await Task.Delay(d, cancellationToken);
        elapsed += d;
        if (elapsed >= totalMs) break;

        yield return (mediumRps, "Medium↑");
        d = Math.Min(mediumSec * 1000, totalMs - elapsed);
        await Task.Delay(d, cancellationToken);
        elapsed += d;
    }
}

static async Task RunWorkersAsync(YdbDataSource dataSource, int targetRps, int workerCount,
    CancellationToken cancellationToken)
{
    // Each rate-limiter window is 100 ms → permitLimit = targetRps / 10
    var permitsPer100Ms = Math.Max(1, targetRps / 10);

    await using var rateLimiter = new FixedWindowRateLimiter(new FixedWindowRateLimiterOptions
    {
        Window = TimeSpan.FromMilliseconds(100),
        PermitLimit = permitsPer100Ms,
        QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
        QueueLimit = permitsPer100Ms * 4
    });

    var workers = Enumerable.Range(0, workerCount).Select(_ => Task.Run(async () =>
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            // ReSharper disable once AccessToDisposedClosure
            using var lease = await rateLimiter.AcquireAsync(1, cancellationToken);
            if (!lease.IsAcquired) continue;
            await Task.Delay(Random.Shared.Next(20));

            try
            {
                await dataSource.ExecuteAsync(async conn =>
                    await new YdbCommand("SELECT 1;", conn).ExecuteNonQueryAsync(cancellationToken));
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch
            {
                /* ignored */
            }
        }
    }, cancellationToken));

    await Task.WhenAll(workers);
}