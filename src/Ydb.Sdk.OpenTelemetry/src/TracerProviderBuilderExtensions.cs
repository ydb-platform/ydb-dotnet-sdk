using OpenTelemetry.Trace;

namespace Ydb.Sdk.OpenTelemetry;

/// <summary>
/// Extension methods for setting up Ydb.Sdk OpenTelemetry tracing.
/// </summary>
public static class TracerProviderBuilderExtensions
{
    /// <summary>
    /// Subscribes to the Ydb.Sdk activity source to enable OpenTelemetry tracing.
    /// </summary>
    public static TracerProviderBuilder AddYdb(this TracerProviderBuilder builder) => builder.AddSource("Ydb.Sdk");
}
