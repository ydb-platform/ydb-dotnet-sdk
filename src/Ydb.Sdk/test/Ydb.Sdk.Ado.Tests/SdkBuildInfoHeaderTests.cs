using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests;

/// <summary>
/// Verifies the <c>x-ydb-sdk-build-info</c> header chain (Python-compatible:
/// <c>ydb-dotnet-sdk/{V};lib1/x;lib2/y</c>).
///
/// The header is assembled inside <see cref="BaseDriver.GetCallOptions"/>; the SDK never sends
/// per-call <c>ClientInfo</c> for internal calls (Discovery, schema, bulk upsert, etc.) — those
/// fall through with the bare <c>ydb-dotnet-sdk/{V}</c> entry coming from
/// <see cref="DriverConfig.SdkVersion"/>.
/// </summary>
public class SdkBuildInfoHeaderTests
{
    private static readonly string SdkVersion = $"ydb-dotnet-sdk/{YdbSdkVersion.Value}";

    [Fact]
    public async Task GetCallOptions_NoClientInfo_KeepsBaseSdkVersion()
    {
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings());

        Assert.Equal(SdkVersion, GetBuildInfoHeader(options));
    }

    [Fact]
    public async Task GetCallOptions_AdoNetClientInfo_AppendsAdoNetComponent()
    {
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings
        {
            ClientInfo = $"ado-net/{YdbSdkVersion.Value}"
        });

        Assert.Equal($"{SdkVersion};ado-net/{YdbSdkVersion.Value}", GetBuildInfoHeader(options));
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
    public async Task GetCallOptions_FrameworkChain_StacksMultipleComponents()
    {
        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings
        {
            ClientInfo = $"ado-net/{YdbSdkVersion.Value};EntityFrameworkCore.Ydb/1.0.0"
        });

        Assert.Equal(
            $"{SdkVersion};ado-net/{YdbSdkVersion.Value};EntityFrameworkCore.Ydb/1.0.0",
            GetBuildInfoHeader(options));
    }

    [Fact]
    public void YdbSdkVersion_DoesNotIncludeGitRevisionBuildMetadata() =>
        Assert.DoesNotContain('+', YdbSdkVersion.Value);

    private static string GetBuildInfoHeader(CallOptions options) =>
        options.Headers!.Get(Metadata.RpcSdkInfoHeader)!.Value;

    private sealed class TestDriver() : BaseDriver(new DriverConfig(false, "localhost", 2136, "/local"),
        NullLoggerFactory.Instance,
        NullLoggerFactory.Instance.CreateLogger<TestDriver>())
    {
        public async ValueTask<CallOptions> InvokeGetCallOptions(GrpcRequestSettings settings)
        {
            // GetCallOptions is protected; the rest of the BaseDriver surface is too gRPC-heavy
            // for a header-only unit test, so we reach in via reflection.
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
