namespace Ydb.Sdk.Ado.Retry.Delay;

public sealed class DefaultStatusDelayProfile : IStatusDelayProfile
{
    public TimeSpan? GetDelay(StatusCode code, int attempt)
    {
        TimeSpan Calc(TimeSpan baseDelay)
        {
            var baseMs = baseDelay.TotalMilliseconds * Math.Pow(2.0, attempt - 1);
            var jitter = 1.0 + Random.Shared.NextDouble() * 0.5;
            var ms = Math.Min(baseMs * jitter, TimeSpan.FromSeconds(10).TotalMilliseconds);
            return TimeSpan.FromMilliseconds(ms);
        }

        switch (code)
        {
            case StatusCode.BadSession:
                return TimeSpan.Zero;
            case StatusCode.Aborted:
            case StatusCode.Unavailable:
            case StatusCode.SessionBusy:
            case StatusCode.ClientInternalError:
            case StatusCode.ClientTransportUnavailable:
            case StatusCode.ClientTransportTimeout:
                return Calc(TimeSpan.FromMilliseconds(100));
            case StatusCode.Overloaded:
            case StatusCode.ClientResourceExhausted:
            case StatusCode.ClientTransportResourceExhausted:
                return Calc(TimeSpan.FromMilliseconds(500));
            default:
                return null;
        }
    }
}
