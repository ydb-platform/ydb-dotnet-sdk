using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Benchmarks;

[SimpleJob(RuntimeMoniker.Net80)]
[MemoryDiagnoser]
[ThreadingDiagnoser]
public class SessionPoolBenchmark
{
    private TestSessionPool _sessionPool = null!;
    private const int SessionPoolSize = 50;
    private const int ConcurrentTasks = 20;

    [GlobalSetup]
    public void Setup()
    {
        var config = new SessionPoolConfig(MaxSessionPool: SessionPoolSize);

        _sessionPool = new TestSessionPool(NullLogger<TestSessionPool>.Instance, config);
    }

    [GlobalCleanup]
    public async Task Cleanup() => await _sessionPool.DisposeAsync();

    [Benchmark]
    public async Task SingleThreaded_OpenClose()
    {
        var session = await _sessionPool.GetSession();
        await session.Release();
    }

    [Benchmark]
    public async Task MultiThreaded_OpenClose()
    {
        var tasks = new Task[ConcurrentTasks];

        for (var i = 0; i < ConcurrentTasks; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var session = await _sessionPool.GetSession();
                await session.Release();
            });
        }

        await Task.WhenAll(tasks);
    }

    [Benchmark]
    public async Task HighContention_OpenClose()
    {
        const int highContentionTasks = 100;
        var tasks = new Task[highContentionTasks];

        for (var i = 0; i < highContentionTasks; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var session = await _sessionPool.GetSession();
                await session.Release();
            });
        }

        await Task.WhenAll(tasks);
    }

    [Benchmark]
    public async Task SessionReuse_Pattern()
    {
        const int iterations = 10;
        var tasks = new Task[ConcurrentTasks];

        for (var i = 0; i < ConcurrentTasks; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                for (var j = 0; j < iterations; j++)
                {
                    var session = await _sessionPool.GetSession();
                    await session.Release();
                }
            });
        }

        await Task.WhenAll(tasks);
    }
}

internal class TestSessionPool(ILogger<TestSessionPool> logger, SessionPoolConfig config)
    : SessionPool<TestSession>(logger, config)
{
    private volatile int _sessionIdCounter;

    protected override Task<TestSession> CreateSession(CancellationToken cancellationToken = default)
    {
        var sessionId = $"test-session-{Interlocked.Increment(ref _sessionIdCounter)}";
        var session = new TestSession(this, sessionId, nodeId: 1);
        return Task.FromResult(session);
    }
}

internal class TestSession : SessionBase<TestSession>
{
    internal TestSession(SessionPool<TestSession> sessionPool, string sessionId, long nodeId)
        : base(sessionPool, sessionId, nodeId, NullLogger<TestSession>.Instance)
    {
    }

    internal override Task<Status> DeleteSession() => Task.FromResult(Status.Success);
}
