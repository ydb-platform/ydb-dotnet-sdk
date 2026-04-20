namespace System;

using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

public class Election
{
    private static readonly Encoding Utf8 = Encoding.UTF8;

    private static readonly string _nodePath = "/local/electionExample";
    private static readonly string _electionName = "apiLeader";

    private static readonly CoordinationClient _coordinationClient =
        new CoordinationClient(Utils.ConnectionString);

    public static async Task Main(string[] args)
    {
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromSeconds(10));

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

        var session1 = _coordinationClient.CreateSession(_nodePath);
        var session2 = _coordinationClient.CreateSession(_nodePath);
        var session3 = _coordinationClient.CreateSession(_nodePath);

        var semaphore = session1.Semaphore(_electionName);
        await semaphore.Create(1, null, CancellationToken.None);

        try
        {
            await Task.WhenAll(
                RunLeader(session1, 1, "worker-a:starting1", "worker-a:8088", cts.Token),
                RunLeader(session2, 2, "worker-a:starting2", "worker-a:8089", cts.Token),
                RunLeader(session3, 3, "worker-a:starting3", "worker-a:8090", cts.Token),

                PrintCurrentLeader(session1, cts.Token),
                PrintCurrentLeader(session2, cts.Token),
                PrintCurrentLeader(session3, cts.Token),
            );

            await PrintCurrentLeader(session1, CancellationToken.None);
            await PrintCurrentLeader(session2, CancellationToken.None);
            await PrintCurrentLeader(session3, CancellationToken.None);
        }
        finally
        {
            await cts.CancelAsync();

            await session1.Close();
            await session2.Close();
            await session3.Close();

            await _coordinationClient.DropNode(_nodePath, dropCoordinationNodeSettings, CancellationToken.None);
        }
    }

    // ── leader ─────────────────────────────────────────────
    private static async Task RunLeader(
        CoordinationSession session,
        ulong number,
        string campaign,
        string proclaim,
        CancellationToken token)
    {
        Console.WriteLine($"[leader {number}] campaigning...");

        var election = session.Election(_electionName);
        await using var leadership = await election.Campaign(Utf8.GetBytes(campaign), token);

        Console.WriteLine($"[leader {number}] elected — publishing endpoint");

        await Task.Delay(300, token);

        await leadership.Proclaim(Utf8.GetBytes(proclaim));
        Console.WriteLine($"[leader {number}] proclaimed endpoint: {proclaim}");

        await Task.Delay(2000, token);

        Console.WriteLine($"[leader {number}] resigning");
        // await leadership.Resign(token);

        Console.WriteLine($"[leader {number}] done");
    }

    // ── follower ───────────────────────────────────────────
    private static async Task RunFollower(
        CoordinationSession session,
        ulong number,
        CancellationToken token)
    {
        Console.WriteLine($"[follower {number}] starting");

        var election = session.Election(_electionName);

        try
        {
            await foreach (var state in election.Observe(token))
            {
                var endpoint = Utf8.GetString(state.Data);

                if (state.Data.Length == 0)
                {
                    Console.WriteLine($"[follower {number}] no leader currently");
                    continue;
                }

                if (state.IsMe)
                {
                    Console.WriteLine($"[follower {number}] i am the leader: {endpoint}");
                    return;
                }

                Console.WriteLine($"[follower {number}] current leader: {endpoint}");
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine($"[follower {number}] stopped");
        }
    }

    // ── one-shot leader query ──────────────────────────────
    private static async Task PrintCurrentLeader(
        CoordinationSession session,
        CancellationToken token)
    {
        var election = session.Election(_electionName);
        var leader = await election.Leader(token);

        if (leader != null)
        {
            Console.WriteLine("[query] current leader: " + Utf8.GetString(leader.Data));
        }
        else
        {
            Console.WriteLine("[query] no leader right now");
        }
    }
}