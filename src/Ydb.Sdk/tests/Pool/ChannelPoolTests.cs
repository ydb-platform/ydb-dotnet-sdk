using System.Collections.Concurrent;
using System.Collections.Immutable;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Tests.Pool;

[Trait("Category", "Unit")]
public class ChannelPoolTests
{
    private readonly Mock<IChannelFactory<TestChannel>> _mockChannelFactory = new();
    private readonly ChannelPool<TestChannel> _channelPool;
    private readonly ConcurrentDictionary<string, Mock<TestChannel>> _endpointToMockChannel = new();

    public ChannelPoolTests()
    {
        _channelPool = new ChannelPool<TestChannel>(Utils.GetLoggerFactory(), _mockChannelFactory.Object);
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

    [Fact]
    public async Task ChannelPool_AllMethodsWorkAsExpected()
    {
        const string n1YdbTech = "n1.ydb.tech";
        const string n2YdbTech = "n2.ydb.tech";
        const string n3YdbTech = "n3.ydb.tech";

        var endpoints = ImmutableArray.Create<string>(
            n1YdbTech, n2YdbTech, n3YdbTech, "n4.ydb.tech", "n5.ydb.tech"
        );

        foreach (var endpoint in endpoints)
        {
            Assert.Equal(endpoint, _channelPool.GetChannel(endpoint).ToString());
        }

        _mockChannelFactory.Verify(
            channelPool => channelPool.CreateChannel(It.IsAny<string>()), Times.Exactly(5)
        );

        await _channelPool.RemoveChannels(ImmutableArray.Create(n1YdbTech, n2YdbTech));

        _endpointToMockChannel.Remove(n1YdbTech, out var mockChannel1);
        _endpointToMockChannel.Remove(n2YdbTech, out var mockChannel2);

        mockChannel1!.Verify(channel => channel.Dispose(), Times.Once);
        mockChannel2!.Verify(channel => channel.Dispose(), Times.Once);

        _endpointToMockChannel[n3YdbTech].Verify(channel => channel.Dispose(), Times.Never);

        foreach (var endpoint in endpoints)
        {
            Assert.Equal(endpoint, _channelPool.GetChannel(endpoint).ToString());
        }

        // created two channels
        _mockChannelFactory.Verify(
            channelPool => channelPool.CreateChannel(It.IsAny<string>()), Times.Exactly(7)
        );

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
                .Select(endpoint => Task.Run(() => _channelPool.GetChannel(endpoint)))
                .ToArray()
            : endpoints
                .Select(endpoint => Task.Run(() => _channelPool.GetChannel(endpoint)))
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
    public abstract class TestChannel : ChannelBase, IDisposable
    {
        protected TestChannel() : base("")
        {
        }

        public abstract void Dispose();
    }
}
