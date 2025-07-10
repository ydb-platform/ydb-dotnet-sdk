using System.Collections.Immutable;
using Moq;
using Xunit;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Tests.Pool;

[Trait("Category", "Unit")]
public class EndpointPoolTests
{
    private static readonly ImmutableArray<EndpointSettings> EndpointSettingsList = ImmutableArray.Create(
        new EndpointSettings(1, "n1.ydb.tech", "MAN"),
        new EndpointSettings(2, "n2.ydb.tech", "VLA"),
        new EndpointSettings(3, "n3.ydb.tech", "SAS"),
        new EndpointSettings(4, "n4.ydb.tech", "SAS"),
        new EndpointSettings(5, "n5.ydb.tech", "VLA")
    );

    public class MockRandomUnitTests
    {
        private readonly Mock<IRandom> _mockRandom = new();
        private readonly EndpointPool _endpointPool;

        public MockRandomUnitTests()
        {
            _endpointPool = new EndpointPool(TestUtils.LoggerFactory, _mockRandom.Object);
            _endpointPool.Reset(EndpointSettingsList);
        }

        [Theory]
        [InlineData(1, "n1.ydb.tech")]
        [InlineData(2, "n2.ydb.tech")]
        [InlineData(3, "n3.ydb.tech")]
        [InlineData(4, "n4.ydb.tech")]
        [InlineData(5, "n5.ydb.tech")]
        public void GetEndpoint_WhenResetNewState_ReturnEndpointByNodeId(int nodeId, string endpoint) =>
            Assert.Equal(endpoint, _endpointPool.GetEndpoint(nodeId));

        [Theory]
        [InlineData(1, "n1.ydb.tech")]
        [InlineData(2, "n2.ydb.tech")]
        [InlineData(3, "n3.ydb.tech")]
        [InlineData(4, "n4.ydb.tech")]
        [InlineData(5, "n5.ydb.tech")]
        public void GetEndpoint_WhenPessimizedEndpoint_ReturnEndpointByNodeId(int nodeId, string endpoint)
        {
            _endpointPool.PessimizeEndpoint(endpoint);
            Assert.Equal(endpoint, _endpointPool.GetEndpoint(nodeId));
        }

        [Fact]
        public void GetEndpoint_WhenResetNewState_ReturnRandomEndpoint()
        {
            const string expectedEndpoint = "n3.ydb.tech";

            _mockRandom.Setup(random => random.Next(EndpointSettingsList.Length)).Returns(2);

            Assert.Equal(expectedEndpoint, _endpointPool.GetEndpoint());
            Assert.Equal(expectedEndpoint, _endpointPool.GetEndpoint(6));
            Assert.Equal(expectedEndpoint, _endpointPool.GetEndpoint(-1));
        }

        [Theory]
        [InlineData("n1.ydb.tech")]
        [InlineData("n2.ydb.tech")]
        [InlineData("n3.ydb.tech")]
        [InlineData("n4.ydb.tech")]
        [InlineData("n5.ydb.tech")]
        public void GetEndpoint_WhenResetNewStateThenPessimizedNode_ReturnRandomEndpoint(string endpoint)
        {
            _mockRandom.Setup(random => random.Next(EndpointSettingsList.Length - 1)).Returns(0);

            Assert.False(_endpointPool.PessimizeEndpoint(endpoint));

            for (var i = 0; i < EndpointSettingsList.Length - 1; i++)
            {
                Assert.NotEqual(endpoint, _endpointPool.GetEndpoint());
            }
        }

        [Fact]
        public void GetEndpoint_And_PessimizeEndpoint_WhenResetNewStateThenPessimizedMajorityNodes_ReturnNeedDiscovery()
        {
            _mockRandom.Setup(random => random.Next(EndpointSettingsList.Length - 2)).Returns(2);

            Assert.False(_endpointPool.PessimizeEndpoint("n1.ydb.tech"));
            Assert.False(_endpointPool.PessimizeEndpoint("n5.ydb.tech"));

            Assert.Equal("n4.ydb.tech", _endpointPool.GetEndpoint());

            // More than half of the nodes are pessimized.
            Assert.True(_endpointPool.PessimizeEndpoint("n2.ydb.tech"));
        }

        [Fact]
        public void PessimizeEndpoint_Reset_WhenPessimizedMajorityNodesThenResetAndAddNewNodes_ReturnRandomEndpoint()
        {
            _mockRandom.Setup(random => random.Next(EndpointSettingsList.Length - 4)).Returns(0);

            Assert.False(_endpointPool.PessimizeEndpoint("n1.ydb.tech"));
            Assert.False(_endpointPool.PessimizeEndpoint("n5.ydb.tech"));
            Assert.True(_endpointPool.PessimizeEndpoint("n2.ydb.tech"));
            Assert.True(_endpointPool.PessimizeEndpoint("n4.ydb.tech"));

            Assert.Equal("n3.ydb.tech", _endpointPool.GetEndpoint());
            // return endpoint by nodeId
            Assert.Equal("n1.ydb.tech", _endpointPool.GetEndpoint(1));

            var listNewEndpointSettings = EndpointSettingsList.ToList();

            listNewEndpointSettings.Add(new EndpointSettings(6, "n6.ydb.tech", "VLA"));
            listNewEndpointSettings.Add(new EndpointSettings(7, "n7.ydb.tech", "MAN"));

            _endpointPool.Reset(listNewEndpointSettings.ToImmutableArray());

            for (var it = 0; it < listNewEndpointSettings.Count; it++)
            {
                _mockRandom.Setup(random => random.Next(listNewEndpointSettings.Count)).Returns(it);
                Assert.Equal(listNewEndpointSettings[it].Endpoint, _endpointPool.GetEndpoint());
            }

            Assert.False(_endpointPool.PessimizeEndpoint("n6.ydb.tech"));
            Assert.False(_endpointPool.PessimizeEndpoint("n7.ydb.tech"));

            for (var i = 0; i < EndpointSettingsList.Length; i++)
            {
                _mockRandom.Setup(random => random.Next(EndpointSettingsList.Length)).Returns(i);
                Assert.Equal(EndpointSettingsList[i].Endpoint, _endpointPool.GetEndpoint());
            }
        }

        [Fact]
        public void PessimizeEndpoint_Reset_WhenResetNewNodes_ReturnRemovedNodes()
        {
            var listNewEndpointSettings = EndpointSettingsList.ToList();

            listNewEndpointSettings.RemoveAt(0);
            listNewEndpointSettings.RemoveAt(0);

            listNewEndpointSettings.Add(new EndpointSettings(6, "n6.ydb.tech", "VLA"));
            listNewEndpointSettings.Add(new EndpointSettings(7, "n7.ydb.tech", "MAN"));

            var removed = _endpointPool.Reset(listNewEndpointSettings.ToImmutableArray());

            Assert.Equal(2, removed.Length);
            Assert.Equal("n1.ydb.tech", removed[0]);
            Assert.Equal("n2.ydb.tech", removed[1]);

            for (var i = 0; i < listNewEndpointSettings.Count; i++)
            {
                _mockRandom.Setup(random => random.Next(listNewEndpointSettings.Count)).Returns(i);
                Assert.Equal(listNewEndpointSettings[i].Endpoint, _endpointPool.GetEndpoint());
            }
        }

        [Fact]
        public void PessimizeEndpoint_WhenPessimizedAllNodes_ReturnRandomEndpoint()
        {
            foreach (var endpointSettings in EndpointSettingsList)
            {
                _endpointPool.PessimizeEndpoint(endpointSettings.Endpoint);
            }

            for (var i = 0; i < EndpointSettingsList.Length; i++)
            {
                _mockRandom.Setup(random => random.Next(EndpointSettingsList.Length)).Returns(i);
                Assert.Equal(EndpointSettingsList[i].Endpoint, _endpointPool.GetEndpoint());
            }
        }
    }

    public class ThreadLocalRandomTests
    {
        private readonly EndpointPool _endpointPool = new(TestUtils.LoggerFactory);

        public ThreadLocalRandomTests()
        {
            _endpointPool.Reset(EndpointSettingsList);
        }

        [Theory]
        [InlineData("n1.ydb.tech")]
        [InlineData("n2.ydb.tech")]
        [InlineData("n3.ydb.tech")]
        [InlineData("n4.ydb.tech")]
        [InlineData("n5.ydb.tech")]
        public void EndpointPool_AllMethodsWorkAsExpected(string endpoint)
        {
            var expectedEndpoints = new HashSet<string>
            {
                "n1.ydb.tech", "n2.ydb.tech", "n3.ydb.tech", "n4.ydb.tech", "n5.ydb.tech"
            };

            for (var it = 0; it < 10; it++)
            {
                Assert.Contains(_endpointPool.GetEndpoint(), expectedEndpoints);
            }

            _endpointPool.PessimizeEndpoint(endpoint);

            expectedEndpoints.Remove(endpoint);
            for (var it = 0; it < 100; it++)
            {
                Assert.Contains(_endpointPool.GetEndpoint(), expectedEndpoints);
            }
        }
    }
}
