using System.Collections.Concurrent;
using System.Timers;
using Timer = System.Timers.Timer;

namespace Internal;

public record Token;

[Serializable]
internal class NoTokensAvailableException : Exception
{
    public static NoTokensAvailableException Instance = new();

    public NoTokensAvailableException()
    {
    }

    public NoTokensAvailableException(string? message) : base(message)
    {
    }

    public NoTokensAvailableException(string? message, Exception? innerException) : base(message, innerException)
    {
    }
}

public class TokenBucket
{
    private readonly int _maxTokens;
    private readonly BlockingCollection<Token> _tokens;

    public TokenBucket(int maxNumberOfTokens, int refillRateMilliseconds)
    {
        _maxTokens = maxNumberOfTokens;
        var timer = new Timer(refillRateMilliseconds);
        _tokens = new BlockingCollection<Token>();

        for (var i = 0; i < maxNumberOfTokens; i++) _tokens.Add(new Token());

        timer.AutoReset = true;
        timer.Enabled = true;
        timer.Elapsed += OnTimerElapsed;
    }

    private void OnTimerElapsed(object? sender, ElapsedEventArgs e)
    {
        var token = new Token();
        var refill = _maxTokens - _tokens.Count;
        for (var i = 0; i < refill; i++)
            _tokens.Add(token);
    }

    public void UseToken()
    {
        if (!_tokens.TryTake(out _)) throw NoTokensAvailableException.Instance;
    }
}