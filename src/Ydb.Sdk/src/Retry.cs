namespace Ydb.Sdk;

public class BackoffSettings
{
    public static readonly double MaxBackoffDurationMs = TimeSpan.FromHours(1).TotalMilliseconds;

    private readonly TimeSpan _slotDuration;
    private readonly uint _ceiling;
    private readonly double _uncertainRatio;

    public BackoffSettings(TimeSpan slotDuration, uint ceiling, double uncertainRation)
    {
        _slotDuration = slotDuration;
        _ceiling = ceiling;
        _uncertainRatio = uncertainRation;
    }

    public static readonly BackoffSettings DefaultFastBackoff = new(
        slotDuration: TimeSpan.FromMilliseconds(1),
        ceiling: 10,
        uncertainRation: 0.5
    );

    public static readonly BackoffSettings DefaultSlowBackoff = new(
        slotDuration: TimeSpan.FromSeconds(1),
        ceiling: 6,
        uncertainRation: 0.5
    );

    public TimeSpan CalcBackoff(uint attemptNumber)
    {
        var random = new Random();

        var backoffSlots = 1u << (int)Math.Min(attemptNumber, _ceiling);
        var maxDuration = _slotDuration * backoffSlots;
        var uncertaintyRatio = Math.Max(Math.Min(_uncertainRatio, 1.0), 0.0);
        var uncertaintyMultiplier = random.NextDouble() * uncertaintyRatio - uncertaintyRatio + 1.0;

        var durationMs = Math.Round(maxDuration.TotalMilliseconds * uncertaintyMultiplier);
        var durationFinalMs = Math.Max(Math.Min(durationMs, MaxBackoffDurationMs), 0.0);
        return TimeSpan.FromMilliseconds(durationFinalMs);
    }
}

public enum RetryPolicy
{
    /// <summary>
    /// Do not retry the operation.
    /// </summary>
    None,

    /// <summary>
    /// Allow retries only for idempotent operations.
    /// </summary>
    IdempotentOnly,

    /// <summary>
    /// Allow retries for all operations regardless of idempotence.
    /// </summary>
    Unconditional
}

public record RetryRule(BackoffSettings BackoffSettings, bool DeleteSession, RetryPolicy Policy);

public class RetrySettings
{
    public static readonly RetrySettings DefaultInstance = new();

    public RetrySettings(
        uint maxAttempts = 10,
        BackoffSettings? fastBackoff = null,
        BackoffSettings? slowBackoff = null)
    {
        MaxAttempts = maxAttempts;
        FastBackoff = fastBackoff ?? BackoffSettings.DefaultFastBackoff;
        SlowBackoff = slowBackoff ?? BackoffSettings.DefaultSlowBackoff;
    }

    public uint MaxAttempts { get; }
    private BackoffSettings FastBackoff { get; }
    private BackoffSettings SlowBackoff { get; }

    public bool IsIdempotent { get; set; }

    private static readonly BackoffSettings NoBackoff = new(TimeSpan.Zero, 10, 0.5);

    public RetryRule GetRetryRule(StatusCode statusCode)
    {
        return statusCode switch
        {
            StatusCode.Unspecified => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.BadRequest => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.Unauthorized => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.InternalError => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.Aborted => new RetryRule(FastBackoff, false, RetryPolicy.IdempotentOnly),
            StatusCode.Unavailable => new RetryRule(FastBackoff, false, RetryPolicy.IdempotentOnly),
            StatusCode.Overloaded => new RetryRule(SlowBackoff, false, RetryPolicy.IdempotentOnly),
            StatusCode.SchemeError => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.GenericError => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.Timeout => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.BadSession => new RetryRule(NoBackoff, true, RetryPolicy.IdempotentOnly),
            StatusCode.PreconditionFailed => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.AlreadyExists => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.NotFound => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.SessionExpired => new RetryRule(NoBackoff, true, RetryPolicy.None),
            StatusCode.Cancelled => new RetryRule(FastBackoff, false, RetryPolicy.None),
            StatusCode.Undetermined => new RetryRule(FastBackoff, false, RetryPolicy.IdempotentOnly),
            StatusCode.Unsupported => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.SessionBusy => new RetryRule(FastBackoff, true, RetryPolicy.IdempotentOnly),
            StatusCode.Success => new RetryRule(NoBackoff, false, RetryPolicy.None),
            StatusCode.ClientResourceExhausted => new RetryRule(SlowBackoff, false, RetryPolicy.IdempotentOnly),
            StatusCode.ClientInternalError => new RetryRule(FastBackoff, true, RetryPolicy.IdempotentOnly),
            StatusCode.ClientTransportUnknown => new RetryRule(NoBackoff, true, RetryPolicy.None),
            StatusCode.ClientTransportUnavailable => new RetryRule(FastBackoff, true, RetryPolicy.IdempotentOnly),
            StatusCode.ClientTransportTimeout => new RetryRule(FastBackoff, true, RetryPolicy.IdempotentOnly),
            StatusCode.ClientTransportResourceExhausted => new RetryRule(SlowBackoff, true, RetryPolicy.IdempotentOnly),
            StatusCode.ClientTransportUnimplemented => new RetryRule(NoBackoff, true, RetryPolicy.None),
            _ => throw new ArgumentOutOfRangeException(nameof(statusCode), statusCode, null)
        };
    }
}
