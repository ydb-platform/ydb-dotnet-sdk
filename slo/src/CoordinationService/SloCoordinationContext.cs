using System.Threading.RateLimiting;
using Internal;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace CoordinationService;

public sealed class SloCoordinationContext : ISloContext
{
    private const string NodeName = "slo-coordination";
    private const string SemaphoreName = "versioned-config";
    private const int ReaderCount = 4;
    private const int RateLimitIntervalMs = 100;

    private static readonly ILogger Logger = ISloContext.Factory.CreateLogger<SloCoordinationContext>();

    public async Task Create(CreateConfig createConfig)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(createConfig.WriteTimeout));
        var client = new CoordinationClient(createConfig.ConnectionString);
        var nodePath = GetNodePath(createConfig.ConnectionString);

        await EnsureNode(client, nodePath, cts.Token);

        await using var session = client.CreateSession(
            nodePath,
            new SessionOptions { Description = "coordination-slo-create" });

        var semaphore = session.Semaphore(SemaphoreName);
        var initialPayload = CoordinationPayload.Encode(0, "bootstrap", DateTimeOffset.UtcNow);

        await semaphore.Create(
            limit: 1,
            data: initialPayload,
            cancellationToken: cts.Token);
        await semaphore.Update(initialPayload, cts.Token);

        Logger.LogInformation(
            "Coordination node {NodePath} and semaphore {SemaphoreName} are ready",
            nodePath,
            SemaphoreName);
    }

    public async Task Run(RunConfig runConfig)
    {
        await Create(new CreateConfig(
            runConfig.ConnectionString,
            runConfig.InitialDataCount,
            runConfig.WriteTimeout));

        var client = new CoordinationClient(runConfig.ConnectionString);
        var nodePath = GetNodePath(runConfig.ConnectionString);

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(runConfig.Time));
        using var writeLimiter = NewLimiter(runConfig.WriteRps);
        using var readLimiter = NewLimiter(runConfig.ReadRps);

        var tasks = new List<Task>
        {
            RunWriter(client, nodePath, writeLimiter, runConfig.WriteTimeout, cts),
            RunWatcher(client, nodePath, cts)
        };

        for (var i = 0; i < ReaderCount; i++)
        {
            tasks.Add(RunReader(client, nodePath, i, readLimiter, runConfig.ReadTimeout, cts));
        }

        try
        {
            Logger.LogInformation(
                "Started coordination SLO workload on {NodePath}/{SemaphoreName}",
                nodePath,
                SemaphoreName);

            await Task.WhenAll(tasks);
        }
        catch (CoordinationSloInvariantException)
        {
            await cts.CancelAsync();
            throw;
        }

        Logger.LogInformation("Coordination SLO workload finished");
    }

    private static async Task EnsureNode(CoordinationClient client, string nodePath, CancellationToken token)
    {
        var config = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Strict,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };

        await client.CreateNode(nodePath, config, token);
        await client.AlterNode(nodePath, config, token);
    }

    private static async Task RunWriter(
        CoordinationClient client,
        string nodePath,
        RateLimiter writeLimiter,
        int writeTimeoutSeconds,
        CancellationTokenSource workloadCts)
    {
        await using var session = client.CreateSession(
            nodePath,
            new SessionOptions { Description = "coordination-slo-writer" });

        var semaphore = session.Semaphore(SemaphoreName);
        var version = 0L;

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                using var lease = await writeLimiter.AcquireAsync(
                    cancellationToken: workloadCts.Token);

                if (!lease.IsAcquired)
                {
                    await Task.Delay(Random.Shared.Next(RateLimitIntervalMs / 2), workloadCts.Token);
                    continue;
                }

                using var opCts = CancellationTokenSource.CreateLinkedTokenSource(workloadCts.Token);
                opCts.CancelAfter(TimeSpan.FromSeconds(writeTimeoutSeconds));

                var nextVersion = Interlocked.Increment(ref version);
                var data = CoordinationPayload.Encode(
                    nextVersion,
                    "writer",
                    DateTimeOffset.UtcNow);

                await semaphore.Update(data, opCts.Token);

                Logger.LogDebug(
                    "Updated coordination payload to version {Version}",
                    nextVersion);
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination writer update failed; continuing workload");
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation("Coordination writer stopped");
    }

    private static async Task RunReader(
        CoordinationClient client,
        string nodePath,
        int readerId,
        RateLimiter readLimiter,
        int readTimeoutSeconds,
        CancellationTokenSource workloadCts)
    {
        await using var session = client.CreateSession(
            nodePath,
            new SessionOptions { Description = $"coordination-slo-reader-{readerId}" });

        var semaphore = session.Semaphore(SemaphoreName);
        var guard = new MonotonicVersionGuard($"reader-{readerId}");

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                using var lease = await readLimiter.AcquireAsync(
                    cancellationToken: workloadCts.Token);

                if (!lease.IsAcquired)
                {
                    await Task.Delay(Random.Shared.Next(RateLimitIntervalMs / 2), workloadCts.Token);
                    continue;
                }

                using var opCts = CancellationTokenSource.CreateLinkedTokenSource(workloadCts.Token);
                opCts.CancelAfter(TimeSpan.FromSeconds(readTimeoutSeconds));

                var description = await semaphore.Describe(DescribeSemaphoreMode.DataOnly, opCts.Token);
                var payload = CoordinationPayload.Decode(description.Data);

                guard.Observe(payload);
            }
            catch (CoordinationSloInvariantException ex)
            {
                Logger.LogCritical(ex, "Coordination reader invariant failed");
                await workloadCts.CancelAsync();
                throw;
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination reader {ReaderId} describe failed; continuing workload", readerId);
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation(
            "Coordination reader {ReaderId} stopped at version {Version}",
            readerId,
            guard.LastObservedVersion);
    }

    private static async Task RunWatcher(
        CoordinationClient client,
        string nodePath,
        CancellationTokenSource workloadCts)
    {
        var guard = new MonotonicVersionGuard("watcher");

        while (!workloadCts.IsCancellationRequested)
        {
            try
            {
                await using var session = client.CreateSession(
                    nodePath,
                    new SessionOptions { Description = "coordination-slo-watcher" });

                var semaphore = session.Semaphore(SemaphoreName);
                var watch = await semaphore.WatchSemaphore(
                    DescribeSemaphoreMode.DataOnly,
                    WatchSemaphoreMode.WatchData,
                    workloadCts.Token);

                guard.Observe(CoordinationPayload.Decode(watch.Initial.Data));

                await foreach (var description in watch.Updates.WithCancellation(workloadCts.Token))
                {
                    guard.Observe(CoordinationPayload.Decode(description.Data));
                }
            }
            catch (CoordinationSloInvariantException ex)
            {
                Logger.LogCritical(ex, "Coordination watcher invariant failed");
                await workloadCts.CancelAsync();
                throw;
            }
            catch (OperationCanceledException) when (workloadCts.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Logger.LogWarning(ex, "Coordination watcher failed; recreating watch");
                await DelayAfterTransientFailure(workloadCts.Token);
            }
        }

        Logger.LogInformation(
            "Coordination watcher stopped at version {Version}",
            guard.LastObservedVersion);
    }

    private static FixedWindowRateLimiter NewLimiter(int rps) =>
        new(new FixedWindowRateLimiterOptions
        {
            Window = TimeSpan.FromMilliseconds(RateLimitIntervalMs),
            PermitLimit = Math.Max(1, rps / (1000 / RateLimitIntervalMs)),
            QueueLimit = int.MaxValue
        });

    private static async Task DelayAfterTransientFailure(CancellationToken token)
    {
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(250), token);
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
        }
    }

    private static string GetNodePath(string connectionString)
    {
        var database = new YdbConnectionStringBuilder(connectionString).Database.TrimEnd('/');
        return $"{database}/{NodeName}";
    }
}
