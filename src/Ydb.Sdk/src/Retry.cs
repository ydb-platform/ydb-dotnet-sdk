namespace Ydb.Sdk;

public class BackoffSettings
{
    public static readonly double MaxBackoffDurationMs = TimeSpan.FromHours(1).TotalMilliseconds;

    public BackoffSettings(TimeSpan slotDuration, uint ceiling, double uncertainRation)
    {
        SlotDuration = slotDuration;
        Ceiling = ceiling;
        UncertainRatio = uncertainRation;
    }

    public TimeSpan SlotDuration { get; }
    public uint Ceiling { get; }
    public double UncertainRatio { get; }

    public static BackoffSettings DefaultFastBackoff { get; } = new(
        slotDuration: TimeSpan.FromMilliseconds(1),
        ceiling: 10,
        uncertainRation: 0.5
    );

    public static BackoffSettings DefaultSlowBackoff { get; } = new(
        slotDuration: TimeSpan.FromSeconds(1),
        ceiling: 6,
        uncertainRation: 0.5
    );

    public TimeSpan CalcBackoff(uint attemptNumber)
    {
        var random = new Random();

        var backoffSlots = 1u << (int)Math.Min(attemptNumber, Ceiling);
        var maxDuration = SlotDuration * backoffSlots;
        var uncertaintyRatio = Math.Max(Math.Min(UncertainRatio, 1.0), 0.0);
        var uncertaintyMultiplier = random.NextDouble() * uncertaintyRatio - uncertaintyRatio + 1.0;

        var durationMs = Math.Round(maxDuration.TotalMilliseconds * uncertaintyMultiplier);
        var durationFinalMs = Math.Max(Math.Min(durationMs, MaxBackoffDurationMs), 0.0);
        return TimeSpan.FromMilliseconds(durationFinalMs);
    }
}

public enum Idempotency
{
    /// <summary> No retry </summary>
    None,

    /// <summary> Retry if IsIdempotent is true </summary>
    Idempotent,

    /// <summary> Retry always </summary>
    NonIdempotent
}

public record RetryRule(BackoffSettings BackoffSettings, bool DeleteSession, Idempotency Idempotency);

public class RetrySettings
{
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
    public BackoffSettings FastBackoff { get; }
    public BackoffSettings SlowBackoff { get; }

    public bool IsIdempotent { get; set; }
    public Action<StatusCode>? Callback { get; set; }
    
    private static readonly BackoffSettings NoBackoff = new(TimeSpan.Zero, 10, 0.5);

    public RetryRule GetRetryRule(StatusCode statusCode)
    {
        return statusCode switch
        {
            StatusCode.Unspecified => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.BadRequest => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.Unauthorized => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.InternalError => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.Aborted => new RetryRule(FastBackoff, false, Idempotency.NonIdempotent),
            StatusCode.Unavailable => new RetryRule(FastBackoff, false, Idempotency.NonIdempotent),
            StatusCode.Overloaded => new RetryRule(SlowBackoff, false, Idempotency.NonIdempotent),
            StatusCode.SchemeError => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.GenericError => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.Timeout => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.BadSession => new RetryRule(NoBackoff, true, Idempotency.NonIdempotent),
            StatusCode.PreconditionFailed => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.AlreadyExists => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.NotFound => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.SessionExpired => new RetryRule(NoBackoff, true, Idempotency.None),
            StatusCode.Cancelled => new RetryRule(FastBackoff, false, Idempotency.None),
            StatusCode.Undetermined => new RetryRule(FastBackoff, false, Idempotency.Idempotent),
            StatusCode.Unsupported => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.SessionBusy => new RetryRule(FastBackoff, true, Idempotency.NonIdempotent),
            StatusCode.Success => new RetryRule(NoBackoff, false, Idempotency.None),
            StatusCode.ClientResourceExhausted => new RetryRule(SlowBackoff, false, Idempotency.NonIdempotent),
            StatusCode.ClientInternalError => new RetryRule(FastBackoff, true, Idempotency.Idempotent),
            StatusCode.ClientTransportUnknown => new RetryRule(NoBackoff, true, Idempotency.None),
            StatusCode.ClientTransportUnavailable => new RetryRule(FastBackoff, true, Idempotency.Idempotent),
            StatusCode.ClientTransportTimeout => new RetryRule(FastBackoff, true, Idempotency.Idempotent),
            StatusCode.ClientTransportResourceExhausted => new RetryRule(SlowBackoff, true, Idempotency.NonIdempotent),
            StatusCode.ClientTransportUnimplemented => new RetryRule(NoBackoff, true, Idempotency.None),
            _ => throw new ArgumentOutOfRangeException(nameof(statusCode), statusCode, null)
        };
    }
}