using System.Collections.Concurrent;
using System.Runtime.Serialization;
using Timer = System.Timers.Timer;

namespace slo;

public record Token;

[Serializable]
internal class NoTokensAvailableException : Exception
{
    public NoTokensAvailableException()
    {
    }

    public static NoTokensAvailableException Instance = new(); 

    public NoTokensAvailableException(string? message) : base(message)
    {
    }

    public NoTokensAvailableException(string? message, Exception? innerException) : base(message, innerException)
    {
    }

    protected NoTokensAvailableException(SerializationInfo info, StreamingContext context) : base(info, context)
    {
    }
}

public class TockenBucket
{
    private BlockingCollection<Token> _tokens;
    private readonly Timer _timer;
    private int _maxTokens;

    public TockenBucket(int maxNumberOfTokens, int refillRateMilliseconds)
    {
        _maxTokens = maxNumberOfTokens;
        _timer = new Timer(refillRateMilliseconds);
        _tokens = new BlockingCollection<Token>();

        for (var i = 0; i < maxNumberOfTokens; i++) _tokens.Add(new Token());

        _timer.AutoReset = true;
        _timer.Enabled = true;
        _timer.Elapsed += OnTimerElapsed;
    }

    private void OnTimerElapsed(object? sender, System.Timers.ElapsedEventArgs e)
    {
        var token = new Token();
        var refill = _maxTokens - _tokens.Count;
        for (var i = 0; i < refill; i++)
            _tokens.Add(token);
    }

    public void UseToken()
    {
        if (!_tokens.TryTake(out _))
        {
            throw NoTokensAvailableException.Instance;
        }
    }
}