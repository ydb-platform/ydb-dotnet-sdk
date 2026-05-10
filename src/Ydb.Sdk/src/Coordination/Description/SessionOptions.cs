using Ydb.Sdk.Ado.RetryPolicy;

namespace Ydb.Sdk.Coordination.Description;

public sealed record SessionOptions
{
    public static SessionOptions Default { get; } = new();
    public string Description { get; init; } = "";
    public TimeSpan StartTimeout { get; init; } = TimeSpan.FromSeconds(5);

    public TimeSpan RecoveryWindow { get; init; } = TimeSpan.FromSeconds(10);

    public IRetryPolicy RetryPolicy { get; init; } = YdbRetryPolicy.Default;
}
