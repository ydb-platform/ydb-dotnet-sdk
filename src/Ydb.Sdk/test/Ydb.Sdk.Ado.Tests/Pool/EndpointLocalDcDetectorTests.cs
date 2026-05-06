using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests.Pool;

public class EndpointLocalDcDetectorTests
{
    [Fact]
    public async Task DetectNearestLocationDc_WhenEndpointsEmpty_ThrowsArgumentException()
    {
        var detector = new EndpointLocalDcDetector(TestUtils.LoggerFactory);

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            detector.DetectNearestLocationDc([], TimeSpan.FromMilliseconds(100)));

        Assert.Contains("Empty endpoints list", exception.Message);
    }

    [Fact]
    public async Task DetectNearestLocationDc_WhenSingleLocation_ReturnsImmediately()
    {
        var detector = new EndpointLocalDcDetector(TestUtils.LoggerFactory);
        var endpoints = new[]
        {
            new EndpointInfo(1, false, "dc1-a.example.com", 2136, "dc1"),
            new EndpointInfo(2, false, "dc1-b.example.com", 2136, "dc1")
        };

        var location = await detector.DetectNearestLocationDc(endpoints, TimeSpan.FromMilliseconds(100));

        Assert.Equal("dc1", location);
    }

    [Fact]
    public async Task DetectNearestLocationDc_WhenAllConnectionsFail_ReturnsNull()
    {
        var connector = new FakeTcpConnector();
        connector.SetupThrow("host1", new SocketException());
        connector.SetupThrow("host2", new SocketException());

        var detector = new EndpointLocalDcDetector(TestUtils.LoggerFactory, connector);
        var endpoints = new[]
        {
            new EndpointInfo(1, false, "host1", 2136, "dc1"),
            new EndpointInfo(2, false, "host2", 2136, "dc2")
        };

        var location = await detector.DetectNearestLocationDc(endpoints, TimeSpan.FromMilliseconds(100));

        Assert.Null(location);
    }

    [Fact]
    public async Task DetectNearestLocationDc_WhenOneEndpointConnectsFirst_ReturnsItsLocation()
    {
        await using var fastListener = StartListener();

        var detector = new EndpointLocalDcDetector(TestUtils.LoggerFactory);
        var endpoints = new[]
        {
            // port 65003 is not listening → ECONNREFUSED immediately
            new EndpointInfo(1, false, "127.0.0.1", 65003, "slow-dc"),
            new EndpointInfo(2, false, "127.0.0.1", fastListener.Port, "fast-dc")
        };

        var location = await detector.DetectNearestLocationDc(endpoints, TimeSpan.FromSeconds(1));

        Assert.Equal("fast-dc", location);
    }

    [Fact]
    public async Task DetectNearestLocationDc_WhenWinnerFound_CancelsRemainingTasks()
    {
        // fast-dc connects immediately; slow-dc blocks until its CancellationToken is
        // cancelled.  If cts.Cancel() is NOT called when the winner is found,
        // Task.WhenAll waits for the full 5-second timeout.
        // With cancellation the test should finish in well under 1 second.
        var connector = new FakeTcpConnector();
        connector.SetupImmediate("fast-host");
        connector.SetupBlockUntilCancelled("slow-host");

        var detector = new EndpointLocalDcDetector(TestUtils.LoggerFactory, connector);
        var endpoints = new[]
        {
            new EndpointInfo(1, false, "slow-host", 2136, "slow-dc"),
            new EndpointInfo(2, false, "fast-host", 2136, "fast-dc")
        };

        var timeout = TimeSpan.FromSeconds(5);
        var sw = Stopwatch.StartNew();

        var location = await detector.DetectNearestLocationDc(endpoints, timeout);

        sw.Stop();

        Assert.Equal("fast-dc", location);
        Assert.True(sw.Elapsed < TimeSpan.FromSeconds(2),
            $"Detection took {sw.Elapsed.TotalMilliseconds:F0} ms — cts.Cancel() after winner found may not be working");
    }

    private static TestTcpListener StartListener()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        var acceptTask = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            await Task.Delay(50);
        });

        return new TestTcpListener(listener, acceptTask);
    }

    private sealed class TestTcpListener(TcpListener listener, Task acceptTask) : IAsyncDisposable
    {
        public uint Port => (uint)((IPEndPoint)listener.LocalEndpoint).Port;

        public async ValueTask DisposeAsync()
        {
            listener.Stop();
            try
            {
                await acceptTask;
            }
            catch (SocketException)
            {
            }
            catch (ObjectDisposedException)
            {
            }
            catch (InvalidOperationException)
            {
                // AcceptTcpClientAsync raced with listener.Stop(): the Task.Run in StartListener
                // hadn't started yet when Stop() was called, so the accept call sees a non-listening
                // listener. The connection itself is accepted by the OS backlog, so the test still
                // observes the winner correctly — this only affects cleanup.
            }
        }
    }

    /// <summary>
    /// Fake implementation of <see cref="ITcpConnector"/> for unit tests.
    /// Per-host behaviour is configured via <c>Setup*</c> methods.
    /// </summary>
    private sealed class FakeTcpConnector : ITcpConnector
    {
        private readonly Dictionary<string, Func<CancellationToken, Task>> _handlers = new();

        /// <summary>Host connects successfully without any delay.</summary>
        public void SetupImmediate(string host) =>
            _handlers[host] = _ => Task.CompletedTask;

        /// <summary>Host blocks until the <see cref="CancellationToken"/> is cancelled.</summary>
        public void SetupBlockUntilCancelled(string host) =>
            _handlers[host] = ct => Task.Delay(Timeout.InfiniteTimeSpan, ct);

        /// <summary>Host throws the given exception immediately.</summary>
        public void SetupThrow(string host, Exception ex) =>
            _handlers[host] = _ => Task.FromException(ex);

        public Task ConnectAsync(string host, int port, CancellationToken cancellationToken) =>
            _handlers.TryGetValue(host, out var handler) ? handler(cancellationToken) :
                // Default: throw SocketException (connection refused)
                Task.FromException(new SocketException());
    }
}
