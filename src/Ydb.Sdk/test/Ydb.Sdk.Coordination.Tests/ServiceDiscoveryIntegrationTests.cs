using System.Text;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;
using Ydb.Sdk.Coordination.Watcher;

namespace Ydb.Sdk.Coordination.Tests;

public class ServiceDiscoveryIntegrationTests
{
    private static readonly Encoding Utf8 = Encoding.UTF8;
    private readonly string _nodePath = "/local/ServiceDiscoveryExample";
    private readonly string _semaphoreName = "endpoints";
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);
    private readonly ITestOutputHelper _output;

    public ServiceDiscoveryIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }


    [Fact]
    public async Task ServiceDiscovery()
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
        var coordinationSession1 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession2 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession3 = _coordinationClient.CreateSession(_nodePath);
        var coordinationSession4 = _coordinationClient.CreateSession(_nodePath);

        using var bCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        using var watchCts = new CancellationTokenSource(TimeSpan.FromSeconds(7));
        try
        {
            await Task.WhenAll(
                RunWorker("worker-a:8080", coordinationSession1, cts.Token),
                RunWorker("worker-b:8081", coordinationSession2, bCts.Token),
                RunWorker("worker-c:8082", coordinationSession3, cts.Token),
                WatchEndpoints(coordinationSession4, watchCts.Token)
            );
        }
        finally
        {
            await cts.CancelAsync();
            await bCts.CancelAsync();
            await coordinationSession1.Close();
            await coordinationSession2.Close();
            await coordinationSession3.Close();
            await coordinationSession4.Close();
            await _coordinationClient.DropNode(_nodePath, dropCoordinationNodeSettings, CancellationToken.None);
        }
    }

    private async Task RunWorker(
        string endpoint,
        CoordinationSession coordinationSession,
        CancellationToken token)
    {
        _output.WriteLine($"[worker] {endpoint} starting");

        var semaphore = coordinationSession.Semaphore(_semaphoreName);
        await using var lease = await semaphore.Acquire(1, true, Utf8.GetBytes(endpoint), null, CancellationToken.None);
        _output.WriteLine($"[worker] {endpoint} registered");

        await WaitForCancellation(token);

        _output.WriteLine($"[worker] {endpoint} unregistered");
    }

    private async Task WatchEndpoints(
        CoordinationSession coordinationSession,
        CancellationToken token)
    {
        _output.WriteLine("[watcher] starting");
        await Task.Delay(100, token);

        var semaphore = coordinationSession.Semaphore(_semaphoreName);
        var watch = await semaphore.WatchSemaphore(DescribeSemaphoreMode.WithOwners,
            WatchSemaphoreMode.WatchOwners, token);
        var descriptionInitial = watch.Initial;
        PrintAvailableWorkers(descriptionInitial);
        try
        {
            await foreach (var description in watch.Updates.WithCancellation(token))
            {
                PrintAvailableWorkers(description);
            }
        }
        catch (OperationCanceledException)
        {
            _output.WriteLine("[watcher] canceled");
        }


        _output.WriteLine("[watcher] done");
    }

    private void PrintAvailableWorkers(
        SemaphoreDescriptionClient description)
    {
        var endpoints = description.GetOwnersList()
            .Select(o => Utf8.GetString(o.Data))
            .ToList();
        _output.WriteLine("[watcher] available workers: " +
                          (endpoints.Count > 0
                              ? string.Join(", ", endpoints)
                              : "(none)"));
    }

    private Task WaitForCancellation(CancellationToken token)
    {
        if (token.IsCancellationRequested)
            return Task.CompletedTask;

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        token.Register(() => tcs.TrySetResult());

        return tcs.Task;
    }
}
