using System.Net;
using System.Net.Sockets;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests.Pool;

public class EndpointLocalDcDetectorTests
{
    private readonly EndpointLocalDcDetector _detector = new(TestUtils.LoggerFactory);

    [Fact]
    public async Task DetectNearestLocationDc_WhenEndpointsEmpty_ThrowsArgumentException()
    {
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            _detector.DetectNearestLocationDc([], TimeSpan.FromMilliseconds(100)));

        Assert.Contains("Empty endpoints list", exception.Message);
    }

    [Fact]
    public async Task DetectNearestLocationDc_WhenSingleLocation_ReturnsImmediately()
    {
        var endpoints = new[]
        {
            new EndpointInfo(1, false, "dc1-a.example.com", 2136, "dc1"),
            new EndpointInfo(2, false, "dc1-b.example.com", 2136, "dc1")
        };

        var location = await _detector.DetectNearestLocationDc(endpoints, TimeSpan.FromMilliseconds(100));

        Assert.Equal("dc1", location);
    }

    [Fact]
    public async Task DetectNearestLocationDc_WhenAllConnectionsFail_ReturnsNull()
    {
        var endpoints = new[]
        {
            new EndpointInfo(1, false, "127.0.0.1", 65001, "dc1"),
            new EndpointInfo(2, false, "127.0.0.1", 65002, "dc2")
        };

        var location = await _detector.DetectNearestLocationDc(endpoints, TimeSpan.FromMilliseconds(100));

        Assert.Null(location);
    }

    [Fact]
    public async Task DetectNearestLocationDc_WhenOneEndpointConnectsFirst_ReturnsItsLocation()
    {
        await using var fastListener = await StartListener();

        var endpoints = new[]
        {
            new EndpointInfo(1, false, "127.0.0.1", 65003, "slow-dc"),
            new EndpointInfo(2, false, "127.0.0.1", fastListener.Port, "fast-dc")
        };

        var location = await _detector.DetectNearestLocationDc(endpoints, TimeSpan.FromSeconds(1));

        Assert.Equal("fast-dc", location);
    }

    private static Task<TestTcpListener> StartListener()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();

        var acceptTask = Task.Run(async () =>
        {
            using var client = await listener.AcceptTcpClientAsync();
            await Task.Delay(50);
        });

        return Task.FromResult(new TestTcpListener(listener, acceptTask));
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
        }
    }
}
