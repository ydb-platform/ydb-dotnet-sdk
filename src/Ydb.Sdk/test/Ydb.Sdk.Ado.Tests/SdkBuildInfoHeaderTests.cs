using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Ado.Tests;

[Collection("DisableParallelization")]
public class SdkBuildInfoHeaderTests : IDisposable
{
    public SdkBuildInfoHeaderTests()
    {
        SdkClientInfoRegistry.Reset();
    }

    public void Dispose() => SdkClientInfoRegistry.Reset();

    [Fact]
    public async Task GetCallOptions_DoesNotAddSdkInfoHeader()
    {
        // x-ydb-sdk-build-info is intentionally NOT added by the common path:
        // only Driver Discovery (ListEndpoints) emits the header, with the registry chain merged.
        SdkClientInfoRegistry.Register($"ado-net/{YdbSdkVersion.Value}");
        SdkClientInfoRegistry.Register(Metadata.TopicWriterClientInfo);

        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings());

        Assert.Null(options.Headers!.Get(Metadata.RpcSdkInfoHeader));
    }

    [Fact]
    public void SdkClientInfoRegistry_DeduplicatesRegistrations()
    {
        SdkClientInfoRegistry.Register($"ado-net/{YdbSdkVersion.Value}");
        SdkClientInfoRegistry.Register($"ado-net/{YdbSdkVersion.Value}");
        SdkClientInfoRegistry.Register(Metadata.TopicWriterClientInfo);

        Assert.Equal(
            $"ado-net/{YdbSdkVersion.Value};topic-writer/{YdbSdkVersion.Value}",
            SdkClientInfoRegistry.Chain);
    }

    [Fact]
    public void SdkClientInfoRegistry_UnregisterDecrementsRefCount_RemovesOnlyAfterLastOwner()
    {
        SdkClientInfoRegistry.Register(Metadata.TopicWriterClientInfo);
        SdkClientInfoRegistry.Register(Metadata.TopicWriterClientInfo);
        SdkClientInfoRegistry.Register(Metadata.TopicReaderClientInfo);

        Assert.Equal(
            $"topic-reader/{YdbSdkVersion.Value};topic-writer/{YdbSdkVersion.Value}",
            SdkClientInfoRegistry.Chain);

        SdkClientInfoRegistry.Unregister(Metadata.TopicWriterClientInfo);

        Assert.Equal(
            $"topic-reader/{YdbSdkVersion.Value};topic-writer/{YdbSdkVersion.Value}",
            SdkClientInfoRegistry.Chain);

        SdkClientInfoRegistry.Unregister(Metadata.TopicWriterClientInfo);
        Assert.Equal($"topic-reader/{YdbSdkVersion.Value}", SdkClientInfoRegistry.Chain);

        SdkClientInfoRegistry.Unregister(Metadata.TopicReaderClientInfo);
        Assert.Null(SdkClientInfoRegistry.Chain);
    }

    [Fact]
    public async Task SessionSource_DisposeAsync_RemovesFromRegistry()
    {
        // Registration happens in PoolManager.Get before driver creation; here we simulate that.
        const string frameworkClientInfo = "EntityFrameworkCore.Ydb/1.0.0";

        SdkClientInfoRegistry.Register($"ado-net/{YdbSdkVersion.Value}");
        SdkClientInfoRegistry.Register(frameworkClientInfo);

        var builder = new YdbConnectionStringBuilder("Host=localhost;Port=2136;Database=/local")
        {
            EnableImplicitSession = true,
            ClientInfo = frameworkClientInfo
        };

        var driver = new TestDriver();
        driver.RegisterOwner();

        var source = new ImplicitSessionSource(driver, builder);

        Assert.Equal(
            $"EntityFrameworkCore.Ydb/1.0.0;ado-net/{YdbSdkVersion.Value}",
            SdkClientInfoRegistry.Chain);

        await source.DisposeAsync();

        Assert.Null(SdkClientInfoRegistry.Chain);
    }

    [Fact]
    public void YdbSdkVersion_HasNumericDottedFormat() =>
        Assert.Matches(@"^\d+\.\d+\.\d+$", YdbSdkVersion.Value);

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
