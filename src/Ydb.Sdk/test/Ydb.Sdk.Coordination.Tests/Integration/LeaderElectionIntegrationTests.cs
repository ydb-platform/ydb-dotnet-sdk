using Xunit;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Recipes;

namespace Ydb.Sdk.Coordination.Tests.Integration;

/// <summary>
/// Distribution is emulated by running multiple <see cref="CoordinationClient"/> instances inside a
/// single process — each instance opens its own session, so the server sees them as independent
/// participants competing for the same election semaphore.
/// </summary>
[Collection("CoordinationIntegration")]
public class LeaderElectionIntegrationTests : IAsyncLifetime
{
    private const string NodePath = "test-coord-leader-election";
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(30);

    private CoordinationClient _admin = null!;

    public async Task InitializeAsync()
    {
        _admin = new CoordinationClient(Utils.ConnectionString);
        try { await _admin.DropNodeAsync(NodePath); }
        catch { /* node may not exist */ }
        await _admin.CreateNodeAsync(NodePath, new NodeConfig());
    }

    public async Task DisposeAsync()
    {
        try { await _admin.DropNodeAsync(NodePath); }
        catch { /* best effort */ }
        await _admin.DisposeAsync();
    }

    [Fact]
    public async Task ManyCandidates_OnlyOneLeaderAtATime()
    {
        const string election = "many-candidates";
        const int candidateCount = 5;

        // Each candidate gets its own client (= its own session).
        var clients = Enumerable.Range(0, candidateCount)
            .Select(_ => new CoordinationClient(Utils.ConnectionString))
            .ToArray();
        try
        {
            // Launch all campaigns simultaneously. Only one Campaign() Task should complete; the
            // rest stay parked on the server-side waiter queue.
            var campaigns = clients.Select((c, idx) => c.CampaignAsync(
                    NodePath, election, BitConverter.GetBytes(idx)))
                .ToArray();

            var firstWinner = await Task.WhenAny(campaigns).WaitAsync(TestTimeout);

            // After ~500 ms only one campaign should have completed.
            await Task.Delay(500);
            var elected = campaigns.Where(t => t.IsCompletedSuccessfully).ToArray();
            var only = Assert.Single(elected);
            Assert.Same(firstWinner, only);

            // The remaining 4 must still be waiting.
            Assert.Equal(candidateCount - 1, campaigns.Count(t => !t.IsCompleted));

            // Tear the elected one down so the rest don't hang waiting on a leader that never resigns.
            await (await firstWinner).DisposeAsync();

            // Eventually a second candidate gets promoted.
            var secondWinner = await Task.WhenAny(campaigns.Where(t => t != firstWinner))
                .WaitAsync(TestTimeout);
            await (await secondWinner).DisposeAsync();
        }
        finally
        {
            foreach (var c in clients) await c.DisposeAsync();
        }
    }

    [Fact]
    public async Task LeaderResign_PromotesNextCandidate_InOrder()
    {
        const string election = "ordered-handoff";
        const int n = 4;
        var clients = Enumerable.Range(0, n)
            .Select(_ => new CoordinationClient(Utils.ConnectionString))
            .ToArray();
        try
        {
            var ordered = new List<Task<Leadership>>();
            for (int i = 0; i < n; i++)
            {
                // Stagger campaigns so the server learns the join order. The first one wins
                // immediately; the rest queue in the order they joined.
                ordered.Add(clients[i].CampaignAsync(NodePath, election, BitConverter.GetBytes(i)));
                await Task.Delay(100);
            }

            // Drain leaders one by one — each resign must hand off to exactly one waiting candidate.
            for (int i = 0; i < n; i++)
            {
                var task = ordered[i];
                var leader = await task.WaitAsync(TestTimeout);
                Assert.False(leader.LeadershipLostToken.IsCancellationRequested);

                // Make sure no other candidate has been promoted prematurely.
                var rest = ordered.Skip(i + 1).ToArray();
                await Task.Delay(200);
                Assert.All(rest, t => Assert.False(t.IsCompleted));

                await leader.DisposeAsync();
            }
        }
        finally
        {
            foreach (var c in clients) await c.DisposeAsync();
        }
    }

    [Fact]
    public async Task Observer_SeesLeaderTransition_AfterResign()
    {
        const string election = "observe-transition";

        await using var observerClient = new CoordinationClient(Utils.ConnectionString);
        await using var observer = await observerClient
            .ObserveLeaderAsync(NodePath, election)
            .WaitAsync(TestTimeout);

        Assert.Null(observer.CurrentLeader);

        await using var candidateA = new CoordinationClient(Utils.ConnectionString);
        await using var candidateB = new CoordinationClient(Utils.ConnectionString);

        var changes = new List<LeaderInfo?>();
        using var observeCts = new CancellationTokenSource();
        var observeTask = Task.Run(async () =>
        {
            await foreach (var info in observer.ObserveAsync(observeCts.Token))
                lock (changes) { changes.Add(info); }
        });

        var leaderA = await candidateA
            .CampaignAsync(NodePath, election, "A"u8.ToArray())
            .WaitAsync(TestTimeout);

        await WaitUntil(() =>
        {
            lock (changes) return changes.Any(c => c != null && c.Data.SequenceEqual("A"u8.ToArray()));
        });

        var campaignB = candidateB.CampaignAsync(NodePath, election, "B"u8.ToArray());
        await Task.Delay(300);
        Assert.False(campaignB.IsCompleted);

        await leaderA.DisposeAsync();
        var leaderB = await campaignB.WaitAsync(TestTimeout);

        await WaitUntil(() =>
        {
            lock (changes) return changes.Any(c => c != null && c.Data.SequenceEqual("B"u8.ToArray()));
        });

        await leaderB.DisposeAsync();

        observeCts.Cancel();
        try { await observeTask; } catch (OperationCanceledException) { }
    }

    [Fact]
    public async Task Leader_GetLeader_OnObserver_ReturnsCurrentLeaderData()
    {
        const string election = "current-leader";
        await using var owner = new CoordinationClient(Utils.ConnectionString);
        await using var watcher = new CoordinationClient(Utils.ConnectionString);

        await using var leader = await owner
            .CampaignAsync(NodePath, election, "winner"u8.ToArray())
            .WaitAsync(TestTimeout);

        await using var observer = await watcher
            .ObserveLeaderAsync(NodePath, election)
            .WaitAsync(TestTimeout);

        Assert.NotNull(observer.CurrentLeader);
        Assert.Equal("winner"u8.ToArray(), observer.CurrentLeader!.Data);
        Assert.Equal(leader.SessionId, observer.CurrentLeader.SessionId);
    }

    [Fact]
    public async Task LeadershipLostToken_IsCancelled_AfterResign()
    {
        const string election = "lost-token";
        await using var client = new CoordinationClient(Utils.ConnectionString);

        var leadership = await client
            .CampaignAsync(NodePath, election, "x"u8.ToArray())
            .WaitAsync(TestTimeout);

        Assert.False(leadership.LeadershipLostToken.IsCancellationRequested);

        await leadership.ResignAsync().WaitAsync(TestTimeout);

        Assert.True(leadership.LeadershipLostToken.IsCancellationRequested);
    }

    private static async Task WaitUntil(Func<bool> predicate)
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        while (!predicate())
            await Task.Delay(50, cts.Token);
    }
}
