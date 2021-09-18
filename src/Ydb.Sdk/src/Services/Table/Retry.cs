using System;
using System.Threading.Tasks;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Table
{
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

        private static readonly BackoffSettings _defaultFastBackoff = new BackoffSettings(
            slotDuration: TimeSpan.FromMilliseconds(1),
            ceiling: 10,
            uncertainRation: 0.5
        );

        private static readonly BackoffSettings _defaultSlowBackoff = new BackoffSettings(
            slotDuration: TimeSpan.FromSeconds(1),
            ceiling: 6,
            uncertainRation: 0.5
        );

        public static BackoffSettings DefaultFastBackoff
        {
            get { return _defaultFastBackoff; }
        }

        public static BackoffSettings DefaultSlowBackoff
        {
            get { return _defaultSlowBackoff; }
        }

        public TimeSpan CalcBackoff(uint attemptNumber)
        {
            var random = new Random();

            uint backoffSlots = 1u << (int)Math.Min(attemptNumber, Ceiling);
            TimeSpan maxDuration = SlotDuration * backoffSlots;
            double uncertaintyRatio = Math.Max(Math.Min(UncertainRatio, 1.0), 0.0);
            double uncertaintyMultiplier = random.NextDouble() * uncertaintyRatio - uncertaintyRatio + 1.0;

            double durationMs = Math.Round(maxDuration.TotalMilliseconds * uncertaintyMultiplier);
            double durationFinalMs = Math.Max(Math.Min(durationMs, MaxBackoffDurationMs), 0.0);
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
}
