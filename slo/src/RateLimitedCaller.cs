using Prometheus;

namespace slo;

public class RateLimitedCaller
{
    private readonly TimeSpan _duration;
    private readonly TockenBucket _tokenBucket;

    private readonly int _rate;

    public RateLimitedCaller(int rate, TimeSpan duration = default, int bunchCount = 10)
    {
        _rate = rate;
        _duration = duration;

        _tokenBucket = new TockenBucket(rate/bunchCount, (int)(1000.0f / bunchCount));
    }

    public Task StartCalling(Func<Task> action, Gauge inFlightGauge)
    {
        var endTime = DateTime.Now + _duration;

        // var i = 0;
        
        while (_duration == default || DateTime.Now < endTime)
        {
            // i++;
            while (inFlightGauge.Value > _rate)
                Thread.Sleep(1);
            while (true)
            {
                try
                {
                    _tokenBucket.UseToken();
                    // Console.WriteLine($"{DateTime.Now.Second}:{DateTime.Now.Millisecond} i={i}");
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