using OpenTelemetry.Trace;

namespace Ydb.Sdk.OpenTelemetry;

/// <summary>
/// Extension methods for setting up Ydb.Sdk OpenTelemetry tracing.
/// </summary>
public static class TracerProviderBuilderExtensions
{
    /// <summary>
    /// Subscribes to all Ydb.Sdk activity sources.
    /// </summary>
    public static TracerProviderBuilder AddYdb(this TracerProviderBuilder builder) =>
        builder.AddYdbAdo().AddYdbTopic();

    /// <summary>
    /// Subscribes to the Ydb.Sdk.Ado activity source to enable ADO.NET tracing.
    /// </summary>
    public static TracerProviderBuilder AddYdbAdo(this TracerProviderBuilder builder) =>
        builder.AddSource("Ydb.Sdk.Ado");

    /// <summary>
    /// Subscribes to the Ydb.Sdk.Topic activity source to enable Topic tracing.
    /// </summary>
    public static TracerProviderBuilder AddYdbTopic(this TracerProviderBuilder builder) =>
        builder.AddSource("Ydb.Sdk.Topic");
}
