using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Coordination.Description;

namespace Ydb.Sdk.Coordination.Tests;

public class MutexIntegrationTests
{
    private readonly string _nodePath = "/local/mutexExample";
    private readonly string _mutexName = "jobLock";
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);
    private readonly ITestOutputHelper _output;

    public MutexIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }


    [Fact]
    public async Task Mutex()
    {
        using var cts = new CancellationTokenSource();

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
        var coordinationSession2 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession3 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession4 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession5 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession6 = _coordinationClient.CreateSession(_nodePath);

        try
        {
            await Task.WhenAll(RunWorker("1", coordinationSession1, cts.Token),
                RunWorker("2", coordinationSession2, cts.Token),
                RunWorker("3", coordinationSession3, cts.Token),
                RunWorker("4", coordinationSession4, cts.Token),
                RunWorker("5", coordinationSession5, cts.Token),
                TryWork(coordinationSession6, cts.Token)
            );
        }
        finally
        {
            await cts.CancelAsync();
            await coordinationSession1.Close();
            await coordinationSession2.Close();
            await coordinationSession3.Close();
            await coordinationSession4.Close();
            await coordinationSession5.Close();
            await coordinationSession6.Close();
            await _coordinationClient.DropNode(_nodePath, CancellationToken.None);
        }
    }


    private async Task RunWorker(
        string id,
        CoordinationSession coordinationSession,
        CancellationToken token)
    {
        _output.WriteLine($"[worker-{id}] starting");

        var mutex = coordinationSession.Mutex(_mutexName);
        await using var lockHandle = await mutex.Lock(token); // using

        _output.WriteLine($"[worker-{id}] lock acquired — doing exclusive work");

        await Task.Delay(500, lockHandle.Token);

        _output.WriteLine($"[worker-{id}] work done, releasing");

        _output.WriteLine($"[worker-{id}] done");
    }

    private async Task TryWork(
        CoordinationSession coordinationSession,
        CancellationToken token)
    {
        var mutex = coordinationSession.Mutex(_mutexName);

        var lease = await mutex.TryLock(token);

        if (lease == null)
        {
            _output.WriteLine("[tryLock] mutex is busy — skipping optional work");
            return;
        }

        await using (lease)
        {
            _output.WriteLine("[tryLock] lock acquired — doing optional work");

            await Task.Delay(200, lease.Token);

            _output.WriteLine("[tryLock] optional work done");
        }
    }
}
