using System.Collections.Immutable;
using Moq;
using Xunit;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests.Pool;

public class EndpointPoolTests
{
    private static List<EndpointInfo> CreateEndpointSettingsList() =>
    [
        new(1, false, "n1.ydb.tech", 2136, "MAN"),
        new(2, true, "n2.ydb.tech", 2135, "VLA"),
        new(3, false, "n3.ydb.tech", 2136, "SAS"),
        new(4, true, "n4.ydb.tech", 2135, "SAS"),
        new(5, false, "n5.ydb.tech", 2136, "VLA")
    ];

    public static IEnumerable<object[]> EndpointSettingsListData =>
        CreateEndpointSettingsList().Select(endpointInfo => new object[] { endpointInfo });

    public class MockRandomUnitTests
    {
        private readonly Mock<IRandom> _mockRandom = new();
        private readonly EndpointPool _endpointPool;
        private readonly List<EndpointInfo> _endpointSettingsList = CreateEndpointSettingsList();

        public MockRandomUnitTests()
        {
            _endpointPool = new EndpointPool(TestUtils.LoggerFactory, _mockRandom.Object);
            _endpointPool.Reset(_endpointSettingsList);
        }

        [Theory]
        [MemberData(nameof(EndpointSettingsListData), MemberType = typeof(EndpointPoolTests))]
        public void GetEndpoint_WhenResetNewState_ReturnEndpointByNodeId(EndpointInfo endpointInfo) =>
            Assert.Equal(endpointInfo, _endpointPool.GetEndpoint(endpointInfo.NodeId));

        [Theory]
        [InlineData(1, "http://n1.ydb.tech:2136")]
        [InlineData(2, "https://n2.ydb.tech:2135")]
        [InlineData(3, "http://n3.ydb.tech:2136")]
        [InlineData(4, "https://n4.ydb.tech:2135")]
        [InlineData(5, "http://n5.ydb.tech:2136")]
        public void GetEndpoint_WhenPessimizedEndpoint_ReturnEndpointByNodeId(int nodeId, string endpoint)
        {
            _endpointPool.PessimizeEndpoint(_endpointSettingsList.Single(e => e.NodeId == nodeId));
            Assert.Equal(endpoint, _endpointPool.GetEndpoint(nodeId).Endpoint);
        }

        [Fact]
        public void GetEndpoint_WhenResetNewState_ReturnRandomEndpoint()
        {
            const string expectedEndpoint = "http://n3.ydb.tech:2136";

            _mockRandom.Setup(random => random.Next(_endpointSettingsList.Count)).Returns(2);

            Assert.Equal(expectedEndpoint, _endpointPool.GetEndpoint().Endpoint);
            Assert.Equal(expectedEndpoint, _endpointPool.GetEndpoint(6).Endpoint);
            Assert.Equal(expectedEndpoint, _endpointPool.GetEndpoint(-1).Endpoint);
        }

        [Theory]
        [InlineData("http://n1.ydb.tech:2136")]
        [InlineData("https://n2.ydb.tech:2135")]
        [InlineData("http://n3.ydb.tech:2136")]
        [InlineData("https://n4.ydb.tech:2135")]
        [InlineData("http://n5.ydb.tech:2136")]
        public void GetEndpoint_WhenResetNewStateThenPessimizedNode_ReturnRandomEndpoint(string endpoint)
        {
            _mockRandom.Setup(random => random.Next(_endpointSettingsList.Count - 1)).Returns(0);

            Assert.False(_endpointPool.PessimizeEndpoint(
                _endpointSettingsList.Single(e => e.Endpoint == endpoint)));

            for (var i = 0; i < _endpointSettingsList.Count - 1; i++)
            {
                Assert.NotEqual(endpoint, _endpointPool.GetEndpoint().Endpoint);
            }
        }

        [Fact]
        public void GetEndpoint_And_PessimizeEndpoint_WhenResetNewStateThenPessimizedMajorityNodes_ReturnNeedDiscovery()
        {
            _mockRandom.Setup(random => random.Next(_endpointSettingsList.Count - 2)).Returns(2);

            Assert.False(_endpointPool.PessimizeEndpoint(_endpointSettingsList[0]));
            Assert.False(_endpointPool.PessimizeEndpoint(_endpointSettingsList[4]));

            Assert.Equal("https://n4.ydb.tech:2135", _endpointPool.GetEndpoint().Endpoint);

            // More than half of the nodes are pessimized.
            Assert.True(_endpointPool.PessimizeEndpoint(_endpointSettingsList[1]));
        }

        [Fact]
        public void PessimizeEndpoint_Reset_WhenPessimizedMajorityNodesThenResetAndAddNewNodes_ReturnRandomEndpoint()
        {
            _mockRandom.Setup(random => random.Next(_endpointSettingsList.Count - 4)).Returns(0);

            Assert.False(_endpointPool.PessimizeEndpoint(_endpointSettingsList[0]));
            Assert.False(_endpointPool.PessimizeEndpoint(_endpointSettingsList[4]));
            Assert.True(_endpointPool.PessimizeEndpoint(_endpointSettingsList[1]));
            Assert.True(_endpointPool.PessimizeEndpoint(_endpointSettingsList[3]));

            Assert.Equal("http://n3.ydb.tech:2136", _endpointPool.GetEndpoint().Endpoint);
            // return endpoint by nodeId
            Assert.Equal("http://n1.ydb.tech:2136", _endpointPool.GetEndpoint(1).Endpoint);

            var listNewEndpointSettings = CreateEndpointSettingsList().ToList();

            listNewEndpointSettings.Add(new EndpointInfo(6, true, "n6.ydb.tech", 2135, "VLA"));
            listNewEndpointSettings.Add(new EndpointInfo(7, true, "n7.ydb.tech", 2135, "MAN"));

            _endpointPool.Reset([..listNewEndpointSettings]);

            for (var it = 0; it < listNewEndpointSettings.Count; it++)
            {
                _mockRandom.Setup(random => random.Next(listNewEndpointSettings.Count)).Returns(it);
                Assert.Equal(listNewEndpointSettings[it].Endpoint, _endpointPool.GetEndpoint().Endpoint);
            }

            var endpoint6 = listNewEndpointSettings.Single(e => e.NodeId == 6);
            var endpoint7 = listNewEndpointSettings.Single(e => e.NodeId == 7);

            Assert.Equal("https://n6.ydb.tech:2135", endpoint6.Endpoint);
            Assert.Equal("https://n7.ydb.tech:2135", endpoint7.Endpoint);
            Assert.False(_endpointPool.PessimizeEndpoint(endpoint6));
            Assert.False(_endpointPool.PessimizeEndpoint(endpoint7));

            var expectedResetEndpoints = CreateEndpointSettingsList()
                .Select(x => x.Endpoint)
                .ToHashSet();
            for (var i = 0; i < expectedResetEndpoints.Count; i++)
            {
                _mockRandom.Setup(random => random.Next(expectedResetEndpoints.Count)).Returns(i);
                Assert.Contains(_endpointPool.GetEndpoint().Endpoint, expectedResetEndpoints);
            }
        }

        [Fact]
        public void PessimizeEndpoint_Reset_WhenResetNewNodes_ReturnRemovedNodes()
        {
            var listNewEndpointSettings = _endpointSettingsList.ToList();

            listNewEndpointSettings.RemoveAt(0);
            listNewEndpointSettings.RemoveAt(0);

            listNewEndpointSettings.Add(new EndpointInfo(6, true, "n6.ydb.tech", 2135, "VLA"));
            listNewEndpointSettings.Add(new EndpointInfo(7, true, "n7.ydb.tech", 2135, "MAN"));

            var removed = _endpointPool.Reset([..listNewEndpointSettings]);

            Assert.Equal(2, removed.Length);
            Assert.Equal("http://n1.ydb.tech:2136", removed[0].Endpoint);
            Assert.Equal("https://n2.ydb.tech:2135", removed[1].Endpoint);

            for (var i = 0; i < listNewEndpointSettings.Count; i++)
            {
                _mockRandom.Setup(random => random.Next(listNewEndpointSettings.Count)).Returns(i);
                Assert.Equal(listNewEndpointSettings[i].Endpoint, _endpointPool.GetEndpoint().Endpoint);
            }
        }

        [Fact]
        public void PessimizeEndpoint_WhenPessimizedAllNodes_ReturnRandomEndpoint()
        {
            foreach (var endpointSettings in _endpointSettingsList)
            {
                _endpointPool.PessimizeEndpoint(endpointSettings);
            }

            var expectedEndpoints = _endpointSettingsList.Select(x => x.Endpoint).ToHashSet();
            for (var i = 0; i < _endpointSettingsList.Count; i++)
            {
                _mockRandom.Setup(random => random.Next(_endpointSettingsList.Count)).Returns(i);
                Assert.Contains(_endpointPool.GetEndpoint().Endpoint, expectedEndpoints);
            }
        }
    }

    public class ThreadLocalRandomTests
    {
        private readonly EndpointPool _endpointPool = new(TestUtils.LoggerFactory);
        private readonly IReadOnlyList<EndpointInfo> _endpointSettingsList = CreateEndpointSettingsList();

        public ThreadLocalRandomTests()
        {
            _endpointPool.Reset(_endpointSettingsList);
        }

        [Theory]
        [InlineData("http://n1.ydb.tech:2136")]
        [InlineData("https://n2.ydb.tech:2135")]
        [InlineData("http://n3.ydb.tech:2136")]
        [InlineData("https://n4.ydb.tech:2135")]
        [InlineData("http://n5.ydb.tech:2136")]
        public void EndpointPool_AllMethodsWorkAsExpected(string endpoint)
        {
            var expectedEndpoints = new HashSet<string>
            {
                "http://n1.ydb.tech:2136", "https://n2.ydb.tech:2135", "http://n3.ydb.tech:2136",
                "https://n4.ydb.tech:2135", "http://n5.ydb.tech:2136"
            };

            for (var it = 0; it < 10; it++)
            {
                Assert.Contains(_endpointPool.GetEndpoint().Endpoint, expectedEndpoints);
            }

            _endpointPool.PessimizeEndpoint(_endpointSettingsList.Single(e => e.Endpoint == endpoint));

            expectedEndpoints.Remove(endpoint);
            for (var it = 0; it < 100; it++)
            {
                Assert.Contains(_endpointPool.GetEndpoint().Endpoint, expectedEndpoints);
            }
        }
    }
}
