namespace Ydb.Sdk.Auth;

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

public abstract class IamProviderBase : ICredentialsProvider
{
    private static readonly TimeSpan IamRefreshInterval = TimeSpan.FromMinutes(5);

    private static readonly TimeSpan IamRefreshGap = TimeSpan.FromMinutes(1);

    private const int IamMaxRetries = 5;

    private readonly object _lock = new();

    private readonly ILogger _logger;

    private volatile IamTokenData? _iamToken;
    private volatile Task? _refreshTask;

    protected IamProviderBase(ILoggerFactory? loggerFactory)
    {
        loggerFactory ??= NullLoggerFactory.Instance;
        _logger = loggerFactory.CreateLogger<IamProviderBase>();
    }

    public async Task Initialize()
    {
        _iamToken = await ReceiveIamToken();
    }

    public string? GetAuthInfo()
    {
        var iamToken = _iamToken;

        if (iamToken is null)
        {
            lock (_lock)
            {
                if (_iamToken is not null) return _iamToken.Token;
                _logger.LogWarning("Blocking for initial IAM token acquirement" +
                                   ", please use explicit Initialize async method.");

                _iamToken = ReceiveIamToken().Result;

                return _iamToken.Token;
            }
        }

        if (iamToken.IsExpired())
        {
            lock (_lock)
            {
                if (!_iamToken!.IsExpired()) return _iamToken.Token;
                _logger.LogWarning("Blocking on expired IAM token.");

                _iamToken = ReceiveIamToken().Result;

                return _iamToken.Token;
            }
        }

        if (!iamToken.IsExpiring() || _refreshTask is not null) return _iamToken!.Token;
        lock (_lock)
        {
            if (!_iamToken!.IsExpiring() || _refreshTask is not null) return _iamToken!.Token;
            _logger.LogInformation("Refreshing IAM token.");

            _refreshTask = Task.Run(RefreshIamToken);
        }

        return _iamToken!.Token;
    }

    private async Task RefreshIamToken()
    {
        var iamToken = await ReceiveIamToken();

        lock (_lock)
        {
            _iamToken = iamToken;
            _refreshTask = null;
        }
    }

    protected async Task<IamTokenData> ReceiveIamToken()
    {
        var retryAttempt = 0;
        while (true)
        {
            try
            {
                _logger.LogTrace($"Attempting to receive IAM token, attempt: {retryAttempt}");

                var iamToken = await FetchToken();

                _logger.LogInformation($"Received IAM token, expires at: {iamToken.ExpiresAt}");

                return iamToken;
            }
            catch (Exception e)
            {
                _logger.LogDebug($"Failed to fetch IAM token, {e}");

                if (retryAttempt >= IamMaxRetries)
                {
                    throw;
                }

                await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));
                ++retryAttempt;
            }
        }
    }

    protected abstract Task<IamTokenData> FetchToken();

    protected class IamTokenData
    {
        public IamTokenData(string token, DateTime expiresAt)
        {
            var now = DateTime.UtcNow;

            Token = token;
            ExpiresAt = expiresAt;

            if (expiresAt <= now)
            {
                RefreshAt = expiresAt;
            }
            else
            {
                var refreshSeconds = new Random().Next((int)IamRefreshInterval.TotalSeconds);
                RefreshAt = expiresAt - IamRefreshGap - TimeSpan.FromSeconds(refreshSeconds);

                if (RefreshAt < now)
                {
                    RefreshAt = expiresAt;
                }
            }
        }

        public string Token { get; }
        public DateTime ExpiresAt { get; }

        public DateTime RefreshAt { get; }

        public bool IsExpired()
        {
            return DateTime.UtcNow >= ExpiresAt;
        }

        public bool IsExpiring()
        {
            return DateTime.UtcNow >= RefreshAt;
        }
    }
}