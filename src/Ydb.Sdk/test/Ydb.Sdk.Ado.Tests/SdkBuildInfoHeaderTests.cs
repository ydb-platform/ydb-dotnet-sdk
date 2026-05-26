using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Session;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests;

public class SdkBuildInfoHeaderTests
{
    private static readonly string SdkVersion = $"ydb-dotnet-sdk/{YdbSdkVersion.Value}";

    [Fact]
    public async Task GetCallOptions_NoClientInfo_KeepsBaseSdkVersion()
    {
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings());

        Assert.Equal(SdkVersion, GetBuildInfoHeader(options));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GetCallOptions_AdoNetSession_AppendsAdoNetComponent(bool enableImplicitSession)
    {
        var clientInfo = GetAdoNetSessionClientInfo(enableImplicitSession, frameworkClientInfo: null);
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings { ClientInfo = clientInfo });

        Assert.Equal($"{SdkVersion};ado-net/{YdbSdkVersion.Value}", GetBuildInfoHeader(options));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task GetCallOptions_FrameworkChain_StacksOnTopOfAdoNet(bool enableImplicitSession)
    {
        const string framework = "EntityFrameworkCore.Ydb/1.0.0";
        var clientInfo = GetAdoNetSessionClientInfo(enableImplicitSession, framework);
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings { ClientInfo = clientInfo });

        Assert.Equal($"{SdkVersion};ado-net/{YdbSdkVersion.Value};{framework}", GetBuildInfoHeader(options));
    }

    [Fact]
    public async Task GetCallOptions_TopicWriterClientInfo_AppendsWriterComponent()
    {
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings
        {
            ClientInfo = Metadata.TopicWriterClientInfo
        });

        Assert.Equal($"{SdkVersion};topic-writer/{YdbSdkVersion.Value}", GetBuildInfoHeader(options));
    }

    [Fact]
    public async Task GetCallOptions_TopicReaderClientInfo_AppendsReaderComponent()
    {
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings
        {
            ClientInfo = Metadata.TopicReaderClientInfo
        });

        Assert.Equal($"{SdkVersion};topic-reader/{YdbSdkVersion.Value}", GetBuildInfoHeader(options));
    }

    [Fact]
    public void YdbSdkVersion_HasNumericDottedFormat() =>
        Assert.Matches(@"^\d+\.\d+\.\d+$", YdbSdkVersion.Value);

    private static string GetAdoNetSessionClientInfo(bool enableImplicitSession, string? frameworkClientInfo)
    {
        var builder = new YdbConnectionStringBuilder("Host=localhost;Port=2136;Database=/local")
        {
            EnableImplicitSession = enableImplicitSession,
            ClientInfo = frameworkClientInfo
        };

        return enableImplicitSession
            ? new ImplicitSessionSource(new TestDriver(), builder).ClientInfo
            : new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(1), builder).ClientInfo;
    }

    private static string GetBuildInfoHeader(CallOptions options) =>
        options.Headers!.Get(Metadata.RpcSdkInfoHeader)!.Value;

    private sealed class TestDriver() : BaseDriver(new DriverConfig(false, "localhost", 2136, "/local"),
        NullLoggerFactory.Instance,
        NullLoggerFactory.Instance.CreateLogger<TestDriver>())
    {
        public async ValueTask<CallOptions> InvokeGetCallOptions(GrpcRequestSettings settings)
        {
            var method = typeof(BaseDriver).GetMethod("GetCallOptions",
                BindingFlags.Instance | BindingFlags.NonPublic)!;

            var endpoint = new EndpointInfo(0, false, "localhost", 2136, "Unknown");
            var task = (ValueTask<CallOptions>)method.Invoke(this, [settings, endpoint])!;
            return await task;
        }

        protected override EndpointInfo GetEndpoint(long nodeId) =>
            new(0, false, "localhost", 2136, string.Empty);

        protected override void OnRpcError(EndpointInfo endpointInfo, RpcException e)
        {
        }
    }
}
