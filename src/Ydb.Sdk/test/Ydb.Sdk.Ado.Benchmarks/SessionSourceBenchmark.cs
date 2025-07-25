using BenchmarkDotNet.Attributes;
using Ydb.Query;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Ado.Benchmarks;

[MemoryDiagnoser]
[ThreadingDiagnoser]
public class SessionSourceBenchmark
{
    private PoolingSessionSource _poolingSessionSource = null!;
    private const int SessionPoolSize = 50;
    private const int ConcurrentTasks = 20;

    [GlobalSetup]
    public void Setup()
    {
        var settings = new YdbConnectionStringBuilder { MaxSessionPool = SessionPoolSize };

        _poolingSessionSource = new PoolingSessionSource(new MockSessionFactory(), settings);
    }

    [Benchmark]
    public async Task SingleThreaded_OpenClose()
    {
        var session = await _poolingSessionSource.OpenSession();
        session.Close();
    }

    [Benchmark]
    public async Task MultiThreaded_OpenClose()
    {
        var tasks = new Task[ConcurrentTasks];

        for (var i = 0; i < ConcurrentTasks; i++)
        {
            tasks[i] = Task.Run(async () =>
            {
                var session = await _poolingSessionSource.OpenSession();
                session.Close();
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
                var session = await _poolingSessionSource.OpenSession();
                session.Close();
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
                    var session = await _poolingSessionSource.OpenSession();
                    session.Close();
                }
            });
        }

        await Task.WhenAll(tasks);
    }
}

internal class MockSessionFactory : IPoolingSessionFactory
{
    public PoolingSession NewSession(PoolingSessionSource source) => new PoolingSession(null, null, false, null);
}
