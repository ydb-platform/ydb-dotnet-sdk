namespace Ydb.Sdk.Ado.RetryPolicy;

public class YdbRetryPolicyConfig
{
    public static readonly YdbRetryPolicyConfig Default = new();

    public int MaxAttempts { get; init; } = 10;

    public int FastBackoffBaseMs { get; init; } = 5;

    public int SlowBackoffBaseMs { get; init; } = 50;

    public int FastCapBackoffMs { get; init; } = 500;

    public int SlowCapBackoffMs { get; init; } = 5_000;

    public bool EnableRetryIdempotence { get; init; } = false;

    public override string ToString() => $"MaxAttempt={MaxAttempts};" +
                                         $"FastBackoffBaseMs={FastBackoffBaseMs};" +
                                         $"SlowBackoffBaseMs={SlowBackoffBaseMs};" +
                                         $"FastCapBackoffMs={FastCapBackoffMs};" +
                                         $"SlowCapBackoffMs={SlowCapBackoffMs};" +
                                         $"EnableRetryIdempotence={EnableRetryIdempotence}";
}
