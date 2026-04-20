namespace System;

public class MutexExample
{
    private static readonly string _nodePath = "/local/mutexExample";
    private static readonly string _mutexName = "jobLock";

    private static readonly CoordinationClient _coordinationClient =
        new CoordinationClient(Utils.ConnectionString);

    public static async Task Main(string[] args)
    {
        using var cts = new CancellationTokenSource();

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
        var s5 = _coordinationClient.CreateSession(_nodePath);
        var s6 = _coordinationClient.CreateSession(_nodePath);

        try
        {
            await Task.WhenAll(
                RunWorker("1", s1, cts.Token),
                RunWorker("2", s2, cts.Token),
                RunWorker("3", s3, cts.Token),
                RunWorker("4", s4, cts.Token),
                RunWorker("5", s5, cts.Token),
                TryWork(s6, cts.Token)
            );
        }
        finally
        {
            await cts.CancelAsync();

            await s1.Close();
            await s2.Close();
            await s3.Close();
            await s4.Close();
            await s5.Close();
            await s6.Close();

            await _coordinationClient.DropNode(_nodePath, dropCoordinationNodeSettings, CancellationToken.None);
        }
    }

    private static async Task RunWorker(
        string id,
        CoordinationSession session,
        CancellationToken token)
    {
        Console.WriteLine($"[worker-{id}] starting");

        var mutex = session.Mutex(_mutexName);
        await using var lockHandle = await mutex.Lock(token);

        Console.WriteLine($"[worker-{id}] lock acquired — doing exclusive work");

        await Task.Delay(500, lockHandle.Token);

        Console.WriteLine($"[worker-{id}] work done, releasing");
        Console.WriteLine($"[worker-{id}] done");
    }

    private static async Task TryWork(
        CoordinationSession session,
        CancellationToken token)
    {
        var mutex = session.Mutex(_mutexName);

        var lease = await mutex.TryLock(token);

        if (lease == null)
        {
            Console.WriteLine("[tryLock] mutex is busy — skipping optional work");
            return;
        }

        await using (lease)
        {
            Console.WriteLine("[tryLock] lock acquired — doing optional work");

            await Task.Delay(200, lease.Token);

            Console.WriteLine("[tryLock] optional work done");
        }
    }
}
