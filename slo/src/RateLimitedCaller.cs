using Prometheus;

namespace slo;

public class RateLimitedCaller
{
    private readonly TimeSpan _duration;

    private readonly int _rate;
    private readonly TokenBucket _tokenBucket;

    public RateLimitedCaller(int rate, TimeSpan duration = default, int bunchCount = 10)
    {
        _rate = rate;
        _duration = duration;

        _tokenBucket = new TokenBucket(rate / bunchCount, (int)(1000.0f / bunchCount));
    }

    public Task StartCalling(Func<Task> action, Gauge inFlightGauge)
    {
        var endTime = DateTime.Now + _duration;

        while (_duration == default || DateTime.Now < endTime)
        {
            while (inFlightGauge.Value > _rate)
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