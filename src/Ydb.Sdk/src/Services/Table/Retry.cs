using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Table;

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
}

public partial class TableClient
{
    public async Task<IResponse> SessionExec(
        Func<Session, Task<IResponse>> operationFunc,
        RetrySettings? retrySettings = null)
    {
        retrySettings ??= new RetrySettings();

        IResponse response = new ClientInternalErrorResponse("SessionRetry, unexpected response value.");
        Session? session = null;
        try
        {
            for (uint attemptNumber = 0; attemptNumber < retrySettings.MaxAttempts; ++attemptNumber)
            {
                if (session is null)
                {
                    var sessionResponse = await _sessionPool.GetSession();
                    response = sessionResponse;

                    if (sessionResponse.Status.IsSuccess)
                    {
                        session = sessionResponse.Result;
                    }
                }

                if (session != null)
                {
                    var operationResponse = await operationFunc(session);
                    if (operationResponse.Status.IsSuccess)
                    {
                        return operationResponse;
                    }

                    response = operationResponse;
                }

                switch (response.Status.StatusCode)
                {
                    case StatusCode.Aborted:
                    case StatusCode.Unavailable:
                        await Task.Delay(retrySettings.FastBackoff.CalcBackoff(attemptNumber));
                        break;

                    case StatusCode.Overloaded:
                    case StatusCode.ClientResourceExhausted:
                    case StatusCode.ClientTransportResourceExhausted:
                        await Task.Delay(retrySettings.SlowBackoff.CalcBackoff(attemptNumber));
                        break;

                    case StatusCode.BadSession:
                    case StatusCode.SessionBusy:
                        if (session != null)
                        {
                            session.Dispose();
                        }

                        session = null;
                        break;

                    default:
                        return response;
                }
            }
        }
        finally
        {
            if (session != null)
            {
                session.Dispose();
            }
        }

        return response;
    }
}