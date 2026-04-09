using OpenTelemetry.Metrics;

namespace Ydb.Sdk.OpenTelemetry;

/// <summary>
/// Extension methods for subscribing to YDB ADO client metrics emitted from the <c>Ydb.Sdk</c> meter.
/// </summary>
public static class MeterProviderBuilderExtensions
{
    /// <summary>
    /// Registers the <see href="https://opentelemetry.io/docs/specs/semconv/database/database-metrics/">database client</see>
    /// meter used by <c>Ydb.Sdk</c> (histograms, counters, connection pool observables).
    /// </summary>
    public static MeterProviderBuilder AddYdb(this MeterProviderBuilder builder) => builder.AddMeter("Ydb.Sdk");
}
