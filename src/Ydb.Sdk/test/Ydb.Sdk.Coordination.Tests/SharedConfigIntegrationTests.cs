using System.Text;
using System.Text.Json;
using Xunit;
using Xunit.Abstractions;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.Tests;

public class SharedConfigIntegrationTests
{
    private readonly string _nodePath = "/local/sharedConfigExample";
    private readonly string _semaphoreName = "config";
    private readonly CoordinationClient _coordinationClient = new(Utils.ConnectionString);
    private readonly ITestOutputHelper _output;

    public SharedConfigIntegrationTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task SharedConfig()
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(15));

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
        var semaphore = coordinationSession1.Semaphore(_semaphoreName);
        await semaphore.Create(1, "{}"u8.ToArray(), CancellationToken.None);

        try
        {
            await Task.WhenAll(
                PublishUpdates(cts.Token), WatchConfig(coordinationSession1, cts.Token)
            );
        }
        finally
        {
            await semaphore.Delete(false, CancellationToken.None);
            await coordinationSession1.Close();
            await _coordinationClient.DropNode(_nodePath, CancellationToken.None);
        }
    }

    // ── publisher ────────────────────────────────────────────────

    private async Task PublishConfig(
        object config,
        CancellationToken token)
    {
        var coordinationSession = _coordinationClient.CreateSession(_nodePath);
        _output.WriteLine("[publisher] start:");
        var semaphore = coordinationSession.Semaphore(_semaphoreName);
        var bytes = Encoding.UTF8.GetBytes(
            JsonSerializer.Serialize(config));
        await semaphore.Update(bytes, token);
        _output.WriteLine($"[publisher] published: {JsonSerializer.Serialize(config)}");
        await coordinationSession.Close();
    }

    // ── watcher ──────────────────────────────────────────────────

    private async Task WatchConfig(
        CoordinationSession coordinationSession,
        CancellationToken token)
    {
        _output.WriteLine("[watcher] starting");
        var semaphore = coordinationSession.Semaphore(_semaphoreName);

        var watch = await semaphore.WatchSemaphore(DescribeSemaphoreMode.DataOnly,
            WatchSemaphoreMode.WatchData, token);
        HandleConfigUpdate(watch.Initial);
        try
        {
            await foreach (var description in watch.Updates.WithCancellation(token))
            {
                HandleConfigUpdate(description);
            }
        }
        catch (OperationCanceledException)
        {
            _output.WriteLine("[watcher] canceled");
        }
    }


    private void HandleConfigUpdate(SemaphoreDescription description)
    {
        if (description.Data.Length == 0)
        {
            _output.WriteLine("[watcher] no config yet");
            return;
        }

        var json = Encoding.UTF8.GetString(description.Data);
        _output.WriteLine(description.Data.ToString());
        try
        {
            var config = JsonSerializer.Deserialize<object>(json);
            _output.WriteLine($"[watcher] config updated: {config}");
        }
        catch (JsonException)
        {
            _output.WriteLine($"[watcher] invalid config json: {json}");
        }
    }
    // ── publish updates ──────────────────────────────────────────

    private async Task PublishUpdates(
        CancellationToken token)
    {
        var configs = new[]
        {
            new { version = 1, logLevel = "info", timeout = 5000 },
            new { version = 2, logLevel = "debug", timeout = 3000 },
            new { version = 3, logLevel = "warn", timeout = 10000 }
        };

        foreach (var config in configs)
        {
            await Task.Delay(500, token);
            await PublishConfig(config, token);
            _output.WriteLine("Published config");
        }
    }
}
