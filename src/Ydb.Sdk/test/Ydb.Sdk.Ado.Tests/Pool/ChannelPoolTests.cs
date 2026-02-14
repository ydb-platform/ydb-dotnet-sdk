using System.Collections.Concurrent;
using System.Collections.Immutable;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests.Pool;

public class ChannelPoolTests
{
    private readonly Mock<IChannelFactory<TestChannel>> _mockChannelFactory = new();
    private readonly ChannelPool<TestChannel> _channelPool;
    private readonly ConcurrentDictionary<string, Mock<TestChannel>> _endpointToMockChannel = new();

    public ChannelPoolTests()
    {
        _channelPool = new ChannelPool<TestChannel>(TestUtils.LoggerFactory, _mockChannelFactory.Object);
        _mockChannelFactory
            .Setup(channelFactory => channelFactory.CreateChannel(It.IsAny<string>()))
            .Returns<string>(endpoint =>
            {
                var mockChannel = new Mock<TestChannel>();

                mockChannel.Setup(channel => channel.ToString()).Returns(endpoint);
                _endpointToMockChannel[endpoint] = mockChannel;

                return mockChannel.Object;
            });
    }

    private static EndpointInfo Endpoint(string host) => new(0, false, host, 2136, string.Empty);

    [Fact]
    public async Task ChannelPool_AllMethodsWorkAsExpected()
    {
        const string n1YdbTech = "n1.ydb.tech";
        const string n2YdbTech = "n2.ydb.tech";
        const string n3YdbTech = "n3.ydb.tech";

        var endpoints = ImmutableArray.Create(
            n1YdbTech, n2YdbTech, n3YdbTech, "n4.ydb.tech", "n5.ydb.tech"
        );

        foreach (var endpoint in endpoints)
        {
            Assert.Equal(Endpoint(endpoint).Endpoint, _channelPool.GetChannel(Endpoint(endpoint)).ToString());
        }

        _mockChannelFactory.Verify(channelPool => channelPool.CreateChannel(It.IsAny<string>()), Times.Exactly(5));

        await _channelPool.RemoveChannels([Endpoint(n1YdbTech), Endpoint(n2YdbTech)]);

        _endpointToMockChannel.Remove(Endpoint(n1YdbTech).Endpoint, out var mockChannel1);
        _endpointToMockChannel.Remove(Endpoint(n2YdbTech).Endpoint, out var mockChannel2);

        mockChannel1!.Verify(channel => channel.Dispose(), Times.Once);
        mockChannel2!.Verify(channel => channel.Dispose(), Times.Once);

        _endpointToMockChannel[Endpoint(n3YdbTech).Endpoint].Verify(channel => channel.Dispose(), Times.Never);

        foreach (var endpoint in endpoints)
        {
            Assert.Equal(Endpoint(endpoint).Endpoint, _channelPool.GetChannel(Endpoint(endpoint)).ToString());
        }

        // created two channels
        _mockChannelFactory.Verify(channelPool => channelPool.CreateChannel(It.IsAny<string>()), Times.Exactly(7));

        await _channelPool.DisposeAsync();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GetChannel_WhenRaceCondition_ChannelIsCreatedOneTime(bool useAsParallel)
    {
        const int endpointCount = 50000;
        var endpoints = new List<string>();

        for (var i = 0; i < 10 * endpointCount; i++)
        {
            endpoints.Add($"n{i % endpointCount}.ydb.tech");
        }

        var tasks = useAsParallel
            ? endpoints.AsParallel()
                .Select(endpoint => Task.Run(() => _channelPool.GetChannel(Endpoint(endpoint))))
                .ToArray()
            : endpoints
                .Select(endpoint => Task.Run(() => _channelPool.GetChannel(Endpoint(endpoint))))
                .ToArray();

        await Task.WhenAll(tasks);

        _mockChannelFactory.Verify(
            channelPool => channelPool.CreateChannel(It.IsAny<string>()), Times.Exactly(endpointCount)
        );

        await _channelPool.DisposeAsync();
    }

    ~ChannelPoolTests()
    {
        foreach (var mockChannel in _endpointToMockChannel.Values)
        {
            mockChannel.Verify(channel => channel.Dispose(), Times.Once);
        }
    }

    // ReSharper disable once MemberCanBePrivate.Global
    public abstract class TestChannel() : ChannelBase(""), IDisposable
    {
        public abstract void Dispose();
    }
}
