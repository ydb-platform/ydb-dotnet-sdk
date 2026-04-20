namespace System;

public class SharedConfigExample
{
    private static readonly string _nodePath = "/local/sharedConfigExample";
    private static readonly string _semaphoreName = "config";

    private static readonly CoordinationClient _coordinationClient =
        new CoordinationClient(Utils.ConnectionString);

    public static async Task Main(string[] args)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

        var config = new NodeConfig
        {
            SelfCheckPeriod = TimeSpan.FromSeconds(1),
            SessionGracePeriod = TimeSpan.FromSeconds(3),
            ReadConsistencyMode = ConsistencyMode.Relaxed,
            AttachConsistencyMode = ConsistencyMode.Relaxed,
            RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
        };
        await _coordinationClient.CreateNode(_nodePath, config, CancellationToken.None);
        var coordinationSession1 = _coordinationClient.CreateSession(_nodePath);
        var semaphore = coordinationSession1.Semaphore(_semaphoreName);
        await semaphore.Create(1, "{}"u8.ToArray(), CancellationToken.None);

        try
        {
            await Task.WhenAll(
                PublishUpdates(cts.Token), WatchConfig(coordinationSession1, cts.Token)
            );
        }
        finally
        {
            await semaphore.Delete(false, CancellationToken.None);
            await coordinationSession1.Close();
            await _coordinationClient.DropNode(_nodePath, CancellationToken.None);
        }
    }

    // ── publisher ─────────────────────────────────────

    private static async Task PublishConfig(object config, CancellationToken token)
    {
        var session = _coordinationClient.CreateSession(_nodePath);

        Console.WriteLine("[publisher] start");

        var semaphore = session.Semaphore(_semaphoreName);

        var json = JsonSerializer.Serialize(config);
        var bytes = Encoding.UTF8.GetBytes(json);

        await semaphore.Update(bytes, token);

        Console.WriteLine($"[publisher] published: {json}");

        await session.Close();
    }

    // ── watcher ───────────────────────────────────────

    private static async Task WatchConfig(
        CoordinationSession session,
        CancellationToken token)
    {
        Console.WriteLine("[watcher] starting");

        var semaphore = session.Semaphore(_semaphoreName);

        var watch = await semaphore.WatchSemaphore(
            DescribeSemaphoreMode.DataOnly,
            WatchSemaphoreMode.WatchData,
            token);

        HandleConfigUpdate(watch.Initial);

        try
        {
            await foreach (var description in watch.Updates.WithCancellation(token))
            {
                HandleConfigUpdate(description);
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[watcher] canceled");
        }
    }

    private static void HandleConfigUpdate(SemaphoreDescriptionClient description)
    {
        if (description.Data.Length == 0)
        {
            Console.WriteLine("[watcher] no config yet");
            return;
        }

        var json = Encoding.UTF8.GetString(description.Data);

        try
        {
            var config = JsonSerializer.Deserialize<object>(json);
            Console.WriteLine($"[watcher] config updated: {json}");
        }
        catch (JsonException)
        {
            Console.WriteLine($"[watcher] invalid config json: {json}");
        }
    }

    // ── publish updates ───────────────────────────────

    private static async Task PublishUpdates(CancellationToken token)
    {
        var configs = new[]
        {
            new { version = 1, logLevel = "info", timeout = 5000 },
            new { version = 2, logLevel = "debug", timeout = 3000 },
            new { version = 3, logLevel = "warn", timeout = 10000 }
        };

        foreach (var config in configs)
        {
            await Task.Delay(500, token);
            await PublishConfig(config, token);
            Console.WriteLine("[publisher] update sent");
        }
    }
}
