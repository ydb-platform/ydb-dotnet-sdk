using System.Reflection;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;
using Ydb.Sdk.Internal;
using Ydb.Sdk.OpenTelemetry;
using Ydb.Sdk.Pool;
using OpenTelemetrySdk = OpenTelemetry.Sdk;
using Metadata = Grpc.Core.Metadata;
using YdbMetadata = Ydb.Sdk.Internal.Metadata;

namespace Ydb.Sdk.Ado.Tests.Internal;

[Collection("DisableParallelization")]
public class SdkBuildInfoHeaderTests
{
    [Fact]
    public async Task GetCallOptions_AddsBaseHeader_OnEveryCall()
    {
        var options = await new TestDriver(clientInfo: null).InvokeGetCallOptions(new GrpcRequestSettings());

        Assert.Equal($"ydb-dotnet-sdk/{YdbSdkVersion.Value}",
            options.Headers!.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    [Fact]
    public async Task GetCallOptions_AddsClientInfoChain_OnEveryCall()
    {
        var clientInfo = $"ado-net/{YdbSdkVersion.Value};ef-core/{YdbSdkVersion.Value}";

        var options = await new TestDriver(clientInfo).InvokeGetCallOptions(new GrpcRequestSettings());

        Assert.Equal($"ydb-dotnet-sdk/{YdbSdkVersion.Value};{clientInfo}",
            options.Headers!.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    [Fact]
    public async Task GetCallOptions_DoesNotAddObservabilityChain_OnRegularCall()
    {
        using var tracerProvider = OpenTelemetrySdk.CreateTracerProviderBuilder().AddYdb().Build();

        var options = await new TestDriver($"ado-net/{YdbSdkVersion.Value}")
            .InvokeGetCallOptions(new GrpcRequestSettings());

        Assert.Equal($"ydb-dotnet-sdk/{YdbSdkVersion.Value};ado-net/{YdbSdkVersion.Value}",
            options.Headers!.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    [Fact]
    public async Task GetCallOptions_ThenAppendObservabilityChain_MatchesDiscoveryPath()
    {
        // Mirrors Driver.DiscoverEndpoints: GetCallOptions (base header) then AppendObservabilityChain.
        using var tracerProvider = OpenTelemetrySdk.CreateTracerProviderBuilder().AddYdb().Build();

        var options = await new TestDriver($"ado-net/{YdbSdkVersion.Value}")
            .InvokeGetCallOptions(new GrpcRequestSettings());
        options.Headers!.AppendObservabilityChain();

        Assert.Equal(
            $"ydb-dotnet-sdk/{YdbSdkVersion.Value};ado-net/{YdbSdkVersion.Value};ydb-sdk-tracing/0.1.0",
            options.Headers!.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    [Fact]
    public void YdbSdkVersion_HasNumericDottedFormat() => Assert.Matches(@"^\d+\.\d+\.\d+$", YdbSdkVersion.Value);

    [Fact]
    public void AppendObservabilityChain_AddsTracingChain_WhenTracingIsEnabled()
    {
        using var provider = OpenTelemetrySdk.CreateTracerProviderBuilder()
            .AddYdb()
            .Build();

        var headers = CreateHeadersWithSdkBuildInfo(clientInfo: null);
        headers.AppendObservabilityChain();

        Assert.Equal($"ydb-dotnet-sdk/{YdbSdkVersion.Value};ydb-sdk-tracing/0.1.0",
            headers.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    [Fact]
    public void AppendObservabilityChain_AddsMetricsChain_WhenMetricsAreEnabled()
    {
        var exportedItems = new List<Metric>();

        using var provider = OpenTelemetrySdk.CreateMeterProviderBuilder()
            .AddYdb()
            .AddInMemoryExporter(exportedItems)
            .Build();

        var headers = CreateHeadersWithSdkBuildInfo(clientInfo: null);
        headers.AppendObservabilityChain();

        Assert.Equal($"ydb-dotnet-sdk/{YdbSdkVersion.Value};ydb-sdk-metrics/0.1.0",
            headers.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    [Fact]
    public void AppendObservabilityChain_AddsClientChain_AndObservabilityChains()
    {
        var exportedItems = new List<Metric>();

        using var tracerProvider = OpenTelemetrySdk.CreateTracerProviderBuilder()
            .AddYdb()
            .Build();
        using var meterProvider = OpenTelemetrySdk.CreateMeterProviderBuilder()
            .AddYdb()
            .AddInMemoryExporter(exportedItems)
            .Build();

        var clientChain = $"ado-net/{YdbSdkVersion.Value}";
        var headers = CreateHeadersWithSdkBuildInfo(clientChain);
        headers.AppendObservabilityChain();

        Assert.Equal(
            $"ydb-dotnet-sdk/{YdbSdkVersion.Value};{clientChain};ydb-sdk-tracing/0.1.0;ydb-sdk-metrics/0.1.0",
            headers.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    [Fact]
    public void AppendObservabilityChain_IsNoOp_WhenObservabilityIsDisabled()
    {
        var clientInfo = $"ado-net/{YdbSdkVersion.Value}";
        var headers = CreateHeadersWithSdkBuildInfo(clientInfo);
        headers.AppendObservabilityChain();

        Assert.Equal($"ydb-dotnet-sdk/{YdbSdkVersion.Value};{clientInfo}",
            headers.Get(YdbMetadata.RpcSdkInfoHeader)?.Value);
    }

    private static Metadata CreateHeadersWithSdkBuildInfo(string? clientInfo) =>
        new DriverConfig(false, "localhost", 2136, "/local", clientInfo: clientInfo).GetCallMetadata;

    private sealed class TestDriver(string? clientInfo) : BaseDriver(
        new DriverConfig(false, "localhost", 2136, "/local", clientInfo: clientInfo),
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
