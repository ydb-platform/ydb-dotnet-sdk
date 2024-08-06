using Prometheus;

namespace Internal;

public class RateLimitedCaller(int rate, TimeSpan duration = default, int bunchCount = 10)
{
    private readonly TokenBucket _tokenBucket = new(rate / bunchCount, (int)(1000.0f / bunchCount));

    public Task StartCalling(Func<Task> action, Gauge inFlightGauge)
    {
        var endTime = DateTime.Now + duration;

        while (duration == default || DateTime.Now < endTime)
        {
            while (inFlightGauge.Value > rate)
            {
                Thread.Sleep(1);
            }

            while (true)
            {
                try
                {
                    _tokenBucket.UseToken();
                    _ = action();
                    break;
                }
                catch (NoTokensAvailableException)
                {
                    Thread.Sleep(1);
                }
            }
        }

        return Task.CompletedTask;
    }
}