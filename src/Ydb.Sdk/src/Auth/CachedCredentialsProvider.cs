using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Auth;

public class CachedCredentialsProvider : ICredentialsProvider
{
    private readonly IClock _clock;
    private readonly IAuthClient _authClient;

    private ILogger<CachedCredentialsProvider> Logger { get; }

    private volatile ITokenState _tokenState;

    public CachedCredentialsProvider(
        IAuthClient authClient,
        ILoggerFactory? loggerFactory = null
    )
    {
        _clock = new SystemClock();
        _authClient = authClient;
        _tokenState = new SyncState(this);
        _tokenState.Init();

        loggerFactory ??= new NullLoggerFactory();
        Logger = loggerFactory.CreateLogger<CachedCredentialsProvider>();
    }

    internal CachedCredentialsProvider(IAuthClient authClient, IClock clock) : this(authClient)
    {
        _clock = clock;
    }

    public async ValueTask<string> GetAuthInfoAsync() =>
        (await _tokenState.Validate(_clock.UtcNow).ConfigureAwait(false)).TokenResponse.Token;

    private Task<TokenResponse> FetchToken() => _authClient.FetchToken();

    private ITokenState UpdateState(ITokenState current, ITokenState next)
    {
        if (Interlocked.CompareExchange(ref _tokenState, next, current) == current)
        {
            next.Init();
        }

        return _tokenState;
    }

    private interface ITokenState
    {
        TokenResponse TokenResponse { get; }

        ValueTask<ITokenState> Validate(DateTime now);

        void Init()
        {
        }
    }

    private class ActiveState(TokenResponse tokenResponse, CachedCredentialsProvider cachedCredentialsProvider)
        : ITokenState
    {
        public TokenResponse TokenResponse { get; } = tokenResponse;

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            if (now < TokenResponse.ExpiredAt)
            {
                return now >= TokenResponse.RefreshAt
                    ? cachedCredentialsProvider.UpdateState(
                        this,
                        new BackgroundState(TokenResponse, cachedCredentialsProvider)
                    )
                    : this;
            }

            cachedCredentialsProvider.Logger.LogWarning(
                "Token has expired. ExpiredAt: {ExpiredAt}, CurrentTime: {CurrentTime}. " +
                "Switching to synchronous state to fetch a new token",
                TokenResponse.ExpiredAt, now
            );

            return await cachedCredentialsProvider
                .UpdateState(this, new SyncState(cachedCredentialsProvider))
                .Validate(now).ConfigureAwait(false);
        }
    }

    private class SyncState(CachedCredentialsProvider cachedCredentialsProvider) : ITokenState
    {
        private readonly TaskCompletionSource<TokenResponse> _fetchTokenResponseTcs = new();

        public TokenResponse TokenResponse =>
            throw new InvalidOperationException("Get token for unfinished sync state");

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            try
            {
                var tokenResponse = await _fetchTokenResponseTcs.Task.ConfigureAwait(false);

                cachedCredentialsProvider.Logger.LogDebug(
                    "Successfully fetched token. ExpiredAt: {ExpiredAt}, RefreshAt: {RefreshAt}",
                    tokenResponse.ExpiredAt, tokenResponse.RefreshAt
                );

                return cachedCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenResponse, cachedCredentialsProvider));
            }
            catch (Exception e)
            {
                cachedCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return cachedCredentialsProvider.UpdateState(this, new ErrorState(e, cachedCredentialsProvider));
            }
        }

        public async void Init()
        {
            try
            {
                var tokenResponse = await cachedCredentialsProvider.FetchToken().ConfigureAwait(false);

                _fetchTokenResponseTcs.SetResult(tokenResponse);
            }
            catch (Exception e)
            {
                _fetchTokenResponseTcs.SetException(e);
            }
        }
    }

    private class BackgroundState(TokenResponse tokenResponse, CachedCredentialsProvider cachedCredentialsProvider)
        : ITokenState
    {
        private readonly TaskCompletionSource<TokenResponse> _fetchTokenResponseTcs = new();

        public TokenResponse TokenResponse { get; } = tokenResponse;

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            var fetchTokenTask = _fetchTokenResponseTcs.Task;

            if (fetchTokenTask.IsCanceled || fetchTokenTask.IsFaulted)
            {
                cachedCredentialsProvider.Logger.LogWarning(
                    "Fetching token task failed. Status: {Status}, Retrying login...",
                    fetchTokenTask.IsCanceled ? "Canceled" : "Faulted"
                );

                return now >= TokenResponse.ExpiredAt
                    ? await cachedCredentialsProvider
                        .UpdateState(this, new SyncState(cachedCredentialsProvider))
                        .Validate(now).ConfigureAwait(false)
                    : cachedCredentialsProvider
                        .UpdateState(this, new BackgroundState(TokenResponse, cachedCredentialsProvider));
            }

            if (fetchTokenTask.IsCompleted)
            {
                return cachedCredentialsProvider
                    .UpdateState(this,
                        new ActiveState(await fetchTokenTask.ConfigureAwait(false), cachedCredentialsProvider));
            }

            if (now < TokenResponse.ExpiredAt)
            {
                return this;
            }

            try
            {
                var tokenResponse = await fetchTokenTask.ConfigureAwait(false);

                cachedCredentialsProvider.Logger.LogDebug(
                    "Successfully fetched token. ExpiredAt: {ExpiredAt}, RefreshAt: {RefreshAt}",
                    tokenResponse.ExpiredAt, tokenResponse.RefreshAt
                );

                return cachedCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenResponse, cachedCredentialsProvider));
            }
            catch (Exception e)
            {
                cachedCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return cachedCredentialsProvider.UpdateState(this, new ErrorState(e, cachedCredentialsProvider));
            }
        }

        public async void Init()
        {
            try
            {
                var tokenResponse = await cachedCredentialsProvider.FetchToken().ConfigureAwait(false);

                _fetchTokenResponseTcs.SetResult(tokenResponse);
            }
            catch (Exception e)
            {
                _fetchTokenResponseTcs.SetException(e);
            }
        }
    }

    private class ErrorState(Exception exception, CachedCredentialsProvider managerCredentialsProvider)
        : ITokenState
    {
        public TokenResponse TokenResponse => throw exception;

        public ValueTask<ITokenState> Validate(DateTime now) => managerCredentialsProvider
            .UpdateState(this, new SyncState(managerCredentialsProvider))
            .Validate(now);
    }
}

public class TokenResponse
{
    private const double RefreshInterval = 0.5;

    public TokenResponse(string token, DateTime expiredAt, DateTime? refreshAt = null)
    {
        var now = DateTime.UtcNow;

        Token = token;
        ExpiredAt = expiredAt.ToUniversalTime();
        RefreshAt = refreshAt?.ToUniversalTime() ?? now + (ExpiredAt - now) * RefreshInterval;
    }

    public string Token { get; }
    public DateTime ExpiredAt { get; }
    public DateTime RefreshAt { get; }
}

public interface IClock
{
    DateTime UtcNow { get; }
}

internal class SystemClock : IClock
{
    public DateTime UtcNow => DateTime.UtcNow;
}
