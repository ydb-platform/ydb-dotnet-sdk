using OpenTelemetry.Metrics;

namespace Ydb.Sdk.OpenTelemetry;

/// <summary>
/// Extension methods for subscribing to YDB client metrics.
/// </summary>
public static class MeterProviderBuilderExtensions
{
    /// <summary>
    /// Registers all meters used by Ydb.Sdk.
    /// </summary>
    public static MeterProviderBuilder AddYdb(this MeterProviderBuilder builder) =>
        builder.AddYdbAdo().AddYdbTopic();

    /// <summary>
    /// Registers the <see href="https://opentelemetry.io/docs/specs/semconv/database/database-metrics/">database client</see>
    /// meter used by Ydb.Sdk ADO.NET (histograms, counters, connection pool observables).
    /// </summary>
    public static MeterProviderBuilder AddYdbAdo(this MeterProviderBuilder builder) => builder.AddMeter("Ydb.Sdk.Ado");

    /// <summary>
    /// Registers the meter used by Ydb.Sdk Topic clients.
    /// </summary>
    public static MeterProviderBuilder AddYdbTopic(this MeterProviderBuilder builder) =>
        builder.AddMeter("Ydb.Sdk.Topic");
}
