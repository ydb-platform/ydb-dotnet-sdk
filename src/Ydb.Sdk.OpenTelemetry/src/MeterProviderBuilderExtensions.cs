using OpenTelemetry.Metrics;

namespace Ydb.Sdk.OpenTelemetry;

public static class MeterProviderBuilderExtensions
{
    public static MeterProviderBuilder AddYdb(this MeterProviderBuilder builder) => builder.AddMeter("Ydb.Sdk");
}
