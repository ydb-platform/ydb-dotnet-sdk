using System.Text;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.Tests;

public class PrimitivesIntegrationTests
{
    private static readonly Encoding Utf8 = Encoding.UTF8;
    private readonly string _nodePath = "/local/electionExample";
    private readonly string _electionName = "apiLeader";
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);
    private readonly ITestOutputHelper _output;

    public PrimitivesIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    /*
    [Fact]
    public async Task LeaderElection1()
    {
        using var cts = new CancellationTokenSource();

        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        await _coordinationClient.CreateNode(_nodePath, coordinationNodeSettings);
        var coordinationSession = _coordinationClient.CreateSession(_nodePath);
        var semaphore = coordinationSession.Semaphore(_electionName);
        await semaphore.Create(2, null);
        //var lease = await semaphore.Acquire(1, false, null, null); //Utf8.GetBytes("worker-a:starting")
        //await lease.Release();
        // var election = coordinationSession.Election(_electionName);
        // var leadership = await election.Campaign(Utf8.GetBytes("worker-a:starting"), new CancellationToken());
        //var lease = await semaphore.Acquire(1, false, Utf8.GetBytes("worker-a:starting"), null);
        //await lease.Release();
        //var lease = await semaphore.Acquire(1, true, null, null);
        // await RunLeader(_coordinationClient, cts.Token);

        await Task.WhenAll(
            RunLeader(_coordinationClient, cts.Token),
            RunLeader2(_coordinationClient, cts.Token),
            RunLeader3(_coordinationClient, cts.Token)
            // RunFollower(_coordinationClient, cts.Token)
        );


        await PrintCurrentLeader(_coordinationClient, cts.Token);

        await coordinationSession.Close();
        await _coordinationClient.DropNode(_nodePath, dropCoordinationNodeSettings);
    }
    */

    [Fact]
    public async Task LeaderElection()
    {
        using var cts = new CancellationTokenSource();

        var coordinationNodeSettings = new CoordinationNodeSettings
        {
            Config = NodeConfig.Create()
                .WithDurationsConfig(TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(3))
                .WithReadConsistencyMode(ConsistencyMode.Relaxed)
                .WithAttachConsistencyMode(ConsistencyMode.Relaxed)
                .WithRateLimiterCountersMode(RateLimiterCountersMode.Detailed)
        };
        var dropCoordinationNodeSettings = new DropCoordinationNodeSettings();
        await _coordinationClient.CreateNode(_nodePath, coordinationNodeSettings);
        var coordinationSession1 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession2 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession3 = _coordinationClient.CreateSession(_nodePath);
        var semaphore = coordinationSession1.Semaphore(_electionName);
        await semaphore.Create(1, null);

        await Task.WhenAll(
            RunLeader(coordinationSession1, 1, "worker-a:starting1", "worker-a:8088", cts.Token),
            RunLeader(coordinationSession2, 2, "worker-a:starting2", "worker-a:8089", cts.Token),
            RunLeader(coordinationSession3, 3, "worker-a:starting3", "worker-a:8090", cts.Token),
            PrintCurrentLeader(coordinationSession1, cts.Token),
            PrintCurrentLeader(coordinationSession2, cts.Token),
            PrintCurrentLeader(coordinationSession3, cts.Token),
            RunFollower(coordinationSession1, 1, cts.Token),
            RunFollower(coordinationSession2, 2, cts.Token),
            RunFollower(coordinationSession3, 3, cts.Token)
        );


        await PrintCurrentLeader(coordinationSession1, cts.Token);
        await PrintCurrentLeader(coordinationSession2, cts.Token);
        await PrintCurrentLeader(coordinationSession3, cts.Token);

        await coordinationSession1.Close();
        await _coordinationClient.DropNode(_nodePath, dropCoordinationNodeSettings);
    }

    // ── leader ────────────────────────────────────────────────────────────────────
    private async Task RunLeader(CoordinationSession coordinationSession, ulong number, string campaign,
        string proclaim,
        CancellationToken token)
    {
        _output.WriteLine("[leader {0}] campaigning...", number);
        var election = coordinationSession.Election(_electionName);
        var leadership = await election.Campaign(Utf8.GetBytes(campaign), token);
        _output.WriteLine("[leader {0}] elected — publishing endpoint", number);

        await Task.Delay(300, token);

        await leadership.Proclaim(Utf8.GetBytes(proclaim));
        _output.WriteLine("[leader {0}] proclaimed endpoint: worker-a:8080", number);

        await Task.Delay(2000, token);

        _output.WriteLine("[leader {0}] resigning", number);
        await leadership.Resign(token);

        _output.WriteLine("[leader {0}] done", number);
    }


    // ── follower ──────────────────────────────────────────────────────────────────
    private async Task RunFollower(CoordinationSession coordinationSession, ulong number, CancellationToken token)
    {
        _output.WriteLine("[follower {0}] starting", number);
        var election = coordinationSession.Election(_electionName);


        await foreach (var state in election.Observe(token))
        {
            var endpoint = Utf8.GetString(state.Data);
            if (state.Data.Length == 0)
            {
                _output.WriteLine("[follower {0}] no leader currently", number);
                continue;
            }

            if (state.IsMe)
            {
                _output.WriteLine("[follower {0}] i am the leader: {1}", number, endpoint);
                return;
            }

            _output.WriteLine("[follower {0}] current leader: {1}", number, endpoint);
        }
    }

    // ── one-shot leader query ─────────────────────────────────────────────────────
    private async Task PrintCurrentLeader(CoordinationSession coordinationSession, CancellationToken token)
    {
        var election = coordinationSession.Election(_electionName);
        var leader = await election.Leader(token);

        if (leader != null)
        {
            _output.WriteLine("[query] current leader: " + Utf8.GetString(leader.Data));
        }
        else
        {
            _output.WriteLine("[query] no leader right now");
        }
    }
}
