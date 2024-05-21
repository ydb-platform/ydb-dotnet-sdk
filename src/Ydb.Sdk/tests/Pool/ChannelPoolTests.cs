using System.Collections.Immutable;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Tests.Pool;

public class ChannelPoolTests
{
    private readonly Mock<IChannelFactory<TestChannel>> _mockChannelFactory = new();
    private readonly ChannelPool<TestChannel> _channelPool;

    public ChannelPoolTests()
    {
        _channelPool = new ChannelPool<TestChannel>(Utils.GetLoggerFactory().CreateLogger<ChannelPool<TestChannel>>(),
            _mockChannelFactory.Object);
    }

    [Fact]
    public async Task ChannelPool_AllMethodsWorkAsExpected()
    {
        var endpointToMockChannel = new Dictionary<string, Mock<TestChannel>>();

        _mockChannelFactory
            .Setup(channelFactory => channelFactory.CreateChannel(It.IsAny<string>()))
            .Returns<string>(endpoint =>
            {
                var mockChannel = new Mock<TestChannel>();

                mockChannel.Setup(channel => channel.ToString()).Returns(endpoint);
                endpointToMockChannel[endpoint] = mockChannel;

                return mockChannel.Object;
            });

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

        endpointToMockChannel.Remove(n1YdbTech, out var mockChannel1);
        endpointToMockChannel.Remove(n2YdbTech, out var mockChannel2);

        mockChannel1!.Verify(channel => channel.Dispose(), Times.Once);
        mockChannel2!.Verify(channel => channel.Dispose(), Times.Once);

        endpointToMockChannel[n3YdbTech].Verify(channel => channel.Dispose(), Times.Never);

        foreach (var endpoint in endpoints)
        {
            Assert.Equal(endpoint, _channelPool.GetChannel(endpoint).ToString());
        }

        // created two channels
        _mockChannelFactory.Verify(
            channelPool => channelPool.CreateChannel(It.IsAny<string>()), Times.Exactly(7)
        );

        await _channelPool.DisposeAsync();

        foreach (var mockChannel in endpointToMockChannel.Values)
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
