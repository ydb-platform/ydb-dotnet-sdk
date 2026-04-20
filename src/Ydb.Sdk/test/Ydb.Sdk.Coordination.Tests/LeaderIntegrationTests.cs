using System.Text;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.Tests;

public class LeaderIntegrationTests
{
    private static readonly Encoding Utf8 = Encoding.UTF8;
    private readonly string _nodePath = "/local/electionExample";
    private readonly string _electionName = "apiLeader";
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);
    private readonly ITestOutputHelper _output;

    public LeaderIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task LeaderElection()
    {
        using var cts = new CancellationTokenSource();
        // Stop everything after 10 seconds
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
        var coordinationSession1 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession2 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession3 = _coordinationClient.CreateSession(_nodePath);
        var semaphore = coordinationSession1.Semaphore(_electionName);
        await semaphore.Create(1, null, CancellationToken.None);

        try
        {
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
            await PrintCurrentLeader(coordinationSession1, CancellationToken.None);
            await PrintCurrentLeader(coordinationSession2, CancellationToken.None);
            await PrintCurrentLeader(coordinationSession3, CancellationToken.None);
        }
        finally
        {
            await cts.CancelAsync();
            await coordinationSession1.Close();
            await coordinationSession2.Close();
            await coordinationSession3.Close();
            await _coordinationClient.DropNode(_nodePath, dropCoordinationNodeSettings, CancellationToken.None);
        }
    }

    // ── leader ────────────────────────────────────────────────────────────────────
    private async Task RunLeader(CoordinationSession coordinationSession, ulong number, string campaign,
        string proclaim,
        CancellationToken token)
    {
        _output.WriteLine("[leader {0}] campaigning...", number);
        var election = coordinationSession.Election(_electionName);
        await using var leadership = await election.Campaign(Utf8.GetBytes(campaign), token);
        _output.WriteLine("[leader {0}] elected — publishing endpoint", number);

        await Task.Delay(300, token);

        await leadership.Proclaim(Utf8.GetBytes(proclaim));
        _output.WriteLine("[leader {0}] proclaimed endpoint: worker-a:8080", number);

        await Task.Delay(2000, token);

        _output.WriteLine("[leader {0}] resigning", number);
        //await leadership.Resign(token);

        _output.WriteLine("[leader {0}] done", number);
    }


    // ── follower ──────────────────────────────────────────────────────────────────
    private async Task RunFollower(CoordinationSession coordinationSession, ulong number, CancellationToken token)
    {
        _output.WriteLine("[follower {0}] starting", number);
        var election = coordinationSession.Election(_electionName);

        try
        {
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
        catch (OperationCanceledException)
        {
            _output.WriteLine("[follower {0}] stopped", number);
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
