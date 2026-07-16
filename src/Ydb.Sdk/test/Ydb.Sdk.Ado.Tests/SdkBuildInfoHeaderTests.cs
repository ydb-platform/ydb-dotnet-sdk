using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;
using Ydb.Sdk.OpenTelemetry;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Internal;
using Ydb.Sdk.Pool;
using OpenTelemetrySdk = OpenTelemetry.Sdk;
using Metadata = Grpc.Core.Metadata;

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
        SdkClientInfoRegistry.Register($"client-a/{YdbSdkVersion.Value}");

        var options = await new TestDriver().InvokeGetCallOptions(new GrpcRequestSettings());

        Assert.Null(options.Headers!.Get("x-ydb-sdk-build-info"));
    }

    [Fact]
    public void SdkClientInfoRegistry_DeduplicatesRegistrations()
    {
        SdkClientInfoRegistry.Register($"ado-net/{YdbSdkVersion.Value}");
        SdkClientInfoRegistry.Register($"ado-net/{YdbSdkVersion.Value}");
        SdkClientInfoRegistry.Register($"client-a/{YdbSdkVersion.Value}");

        Assert.Equal(
            $"ado-net/{YdbSdkVersion.Value};client-a/{YdbSdkVersion.Value}",
            SdkClientInfoRegistry.Chain);
    }

    [Fact]
    public void SdkClientInfoRegistry_UnregisterDecrementsRefCount_RemovesOnlyAfterLastOwner()
    {
        var clientA = $"client-a/{YdbSdkVersion.Value}";
        var clientB = $"client-b/{YdbSdkVersion.Value}";

        SdkClientInfoRegistry.Register(clientA);
        SdkClientInfoRegistry.Register(clientA);
        SdkClientInfoRegistry.Register(clientB);

        Assert.Equal($"{clientA};{clientB}", SdkClientInfoRegistry.Chain);

        SdkClientInfoRegistry.Unregister(clientA);

        Assert.Equal($"{clientA};{clientB}", SdkClientInfoRegistry.Chain);

        SdkClientInfoRegistry.Unregister(clientA);
        Assert.Equal(clientB, SdkClientInfoRegistry.Chain);

        SdkClientInfoRegistry.Unregister(clientB);
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
    public void YdbSdkVersion_HasNumericDottedFormat() => Assert.Matches(@"^\d+\.\d+\.\d+$", YdbSdkVersion.Value);

    [Fact]
    public void AddSdkBuildInfo_AddsBaseHeader_WhenObservabilityIsDisabled()
    {
        var headers = new Metadata();
        headers.AddSdkBuildInfo();

        Assert.Equal($"ydb-dotnet-sdk/{YdbSdkVersion.Value}", headers.Get("x-ydb-sdk-build-info")?.Value);
    }

    [Fact]
    public void AddSdkBuildInfo_AddsTracingChain_WhenTracingIsEnabled()
    {
        using var provider = OpenTelemetrySdk.CreateTracerProviderBuilder()
            .AddYdb()
            .Build();

        var headers = new Metadata();
        headers.AddSdkBuildInfo();

        Assert.Equal(
            $"ydb-dotnet-sdk/{YdbSdkVersion.Value};ydb-sdk-tracing/{ObservabilityInfo.TracingChainVersion}",
            headers.Get("x-ydb-sdk-build-info")?.Value);
    }

    [Fact]
    public void AddSdkBuildInfo_AddsMetricsChain_WhenMetricsAreEnabled()
    {
        var exportedItems = new List<Metric>();

        using var provider = OpenTelemetrySdk.CreateMeterProviderBuilder()
            .AddYdb()
            .AddInMemoryExporter(exportedItems)
            .Build();

        var headers = new Metadata();
        headers.AddSdkBuildInfo();

        Assert.Equal(
            $"ydb-dotnet-sdk/{YdbSdkVersion.Value};ydb-sdk-metrics/{ObservabilityInfo.MetricsChainVersion}",
            headers.Get("x-ydb-sdk-build-info")?.Value);
    }

    [Fact]
    public void AddSdkBuildInfo_AddsObservabilityChains_AndClientChain()
    {
        var exportedItems = new List<Metric>();

        using var tracerProvider = OpenTelemetrySdk.CreateTracerProviderBuilder()
            .AddYdb()
            .Build();
        using var meterProvider = OpenTelemetrySdk.CreateMeterProviderBuilder()
            .AddYdb()
            .AddInMemoryExporter(exportedItems)
            .Build();

        const string clientChain = "ado-net/1.0.0";
        SdkClientInfoRegistry.Register(clientChain);

        var headers = new Metadata();
        headers.AddSdkBuildInfo();

        Assert.Equal(
            $"ydb-dotnet-sdk/{YdbSdkVersion.Value};ydb-sdk-tracing/{ObservabilityInfo.TracingChainVersion};ydb-sdk-metrics/{ObservabilityInfo.MetricsChainVersion};{clientChain}",
            headers.Get("x-ydb-sdk-build-info")?.Value);
    }

    [Fact]
    public void AddSdkBuildInfo_AddsTracingChain_ForManualYdbSourceSubscription()
    {
        using var provider = OpenTelemetrySdk.CreateTracerProviderBuilder()
            .AddSource("Ydb.Sdk")
            .Build();

        var headers = new Metadata();
        headers.AddSdkBuildInfo();

        Assert.Equal(
            $"ydb-dotnet-sdk/{YdbSdkVersion.Value};ydb-sdk-tracing/{ObservabilityInfo.TracingChainVersion}",
            headers.Get("x-ydb-sdk-build-info")?.Value);
    }

    [Fact]
    public void AddSdkBuildInfo_AddsMetricsChain_ForManualYdbMeterSubscription()
    {
        var exportedItems = new List<Metric>();

        using var provider = OpenTelemetrySdk.CreateMeterProviderBuilder()
            .AddMeter("Ydb.Sdk")
            .AddInMemoryExporter(exportedItems)
            .Build();

        var headers = new Metadata();
        headers.AddSdkBuildInfo();

        Assert.Equal(
            $"ydb-dotnet-sdk/{YdbSdkVersion.Value};ydb-sdk-metrics/{ObservabilityInfo.MetricsChainVersion}",
            headers.Get("x-ydb-sdk-build-info")?.Value);
    }

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
