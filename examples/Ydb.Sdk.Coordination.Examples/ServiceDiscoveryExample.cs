namespace System;

public class ServiceDiscoveryExample
{
    private static readonly Encoding Utf8 = Encoding.UTF8;

    private static readonly string _nodePath = "/local/ServiceDiscoveryExample";
    private static readonly string _semaphoreName = "endpoints";

    private static readonly CoordinationClient _coordinationClient =
        new CoordinationClient(Utils.ConnectionString);

    public static async Task Main(string[] args)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));

        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = new NodeConfig
            {
                SelfCheckPeriod = TimeSpan.FromSeconds(1),
                SessionGracePeriod = TimeSpan.FromSeconds(3),
                ReadConsistencyMode = ConsistencyMode.Relaxed,
                AttachConsistencyMode = ConsistencyMode.Relaxed,
                RateLimiterCountersModeValue = RateLimiterCountersMode.Detailed
            }
        };

        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();

        await _coordinationClient.CreateNode(_nodePath, coordinationNodeSettings, CancellationToken.None);

        var s1 = _coordinationClient.CreateSession(_nodePath);
        var s2 = _coordinationClient.CreateSession(_nodePath);
        var s3 = _coordinationClient.CreateSession(_nodePath);
        var s4 = _coordinationClient.CreateSession(_nodePath);

        using var bCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        using var watchCts = new CancellationTokenSource(TimeSpan.FromSeconds(7));

        try
        {
            await Task.WhenAll(
                RunWorker("worker-a:8080", s1, cts.Token),
                RunWorker("worker-b:8081", s2, bCts.Token),
                RunWorker("worker-c:8082", s3, cts.Token),
                WatchEndpoints(s4, watchCts.Token)
            );
        }
        finally
        {
            await cts.CancelAsync();
            await bCts.CancelAsync();

            await s1.Close();
            await s2.Close();
            await s3.Close();
            await s4.Close();

            await _coordinationClient.DropNode(_nodePath, dropCoordinationNodeSettings, CancellationToken.None);
        }
    }

    private static async Task RunWorker(
        string endpoint,
        CoordinationSession session,
        CancellationToken token)
    {
        Console.WriteLine($"[worker] {endpoint} starting");

        var semaphore = session.Semaphore(_semaphoreName);

        await using var lease = await semaphore.Acquire(
            1,
            true,
            Utf8.GetBytes(endpoint),
            null,
            CancellationToken.None);

        Console.WriteLine($"[worker] {endpoint} registered");

        await WaitForCancellation(token);

        Console.WriteLine($"[worker] {endpoint} unregistered");
    }

    private static async Task WatchEndpoints(
        CoordinationSession session,
        CancellationToken token)
    {
        Console.WriteLine("[watcher] starting");

        await Task.Delay(100, token);

        var semaphore = session.Semaphore(_semaphoreName);

        var watch = await semaphore.WatchSemaphore(
            DescribeSemaphoreMode.WithOwners,
            WatchSemaphoreMode.WatchOwners,
            token);

        var initial = watch.Initial;
        PrintAvailableWorkers(initial);

        try
        {
            await foreach (var description in watch.Updates.WithCancellation(token))
            {
                PrintAvailableWorkers(description);
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("[watcher] canceled");
        }

        Console.WriteLine("[watcher] done");
    }

    private static void PrintAvailableWorkers(
        SemaphoreDescriptionClient description)
    {
        var endpoints = description.GetOwnersList()
            .Select(o => Utf8.GetString(o.Data))
            .ToList();

        Console.WriteLine("[watcher] available workers: " +
            (endpoints.Count > 0
                ? string.Join(", ", endpoints)
                : "(none)"));
    }

    private static Task WaitForCancellation(CancellationToken token)
    {
        if (token.IsCancellationRequested)
            return Task.CompletedTask;

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        token.Register(() => tcs.TrySetResult());

        return tcs.Task;
    }
}
