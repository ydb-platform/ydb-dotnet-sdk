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
        (await _tokenState.Validate(_clock.UtcNow)).TokenResponse.Token;

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

    private class InitState : ITokenState
    {
        private readonly SyncState _syncState;

        public InitState(SyncState syncState)
        {
            syncState.Init();
            _syncState = syncState;
        }

        public TokenResponse TokenResponse => _syncState.TokenResponse;

        public async ValueTask<ITokenState> Validate(DateTime now) =>
            await (await _syncState.Validate(now)).Validate(now);
    }

    private class ActiveState : ITokenState
    {
        private readonly CachedCredentialsProvider _cachedCredentialsProvider;

        public ActiveState(TokenResponse tokenResponse, CachedCredentialsProvider cachedCredentialsProvider)
        {
            TokenResponse = tokenResponse;
            _cachedCredentialsProvider = cachedCredentialsProvider;
        }

        public TokenResponse TokenResponse { get; }

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            if (now < TokenResponse.ExpiredAt)
            {
                return now >= TokenResponse.RefreshAt
                    ? _cachedCredentialsProvider.UpdateState(
                        this,
                        new BackgroundState(TokenResponse, _cachedCredentialsProvider)
                    )
                    : this;
            }

            _cachedCredentialsProvider.Logger.LogWarning(
                "Token has expired. ExpiredAt: {ExpiredAt}, CurrentTime: {CurrentTime}. " +
                "Switching to synchronous state to fetch a new token",
                TokenResponse.ExpiredAt, now
            );

            return await _cachedCredentialsProvider
                .UpdateState(this, new SyncState(_cachedCredentialsProvider))
                .Validate(now);
        }
    }

    private class SyncState : ITokenState
    {
        private readonly CachedCredentialsProvider _cachedCredentialsProvider;

        private volatile Task<TokenResponse> _fetchTokenTask = null!;

        public SyncState(CachedCredentialsProvider cachedCredentialsProvider)
        {
            _cachedCredentialsProvider = cachedCredentialsProvider;
        }

        public TokenResponse TokenResponse =>
            throw new InvalidOperationException("Get token for unfinished sync state");

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            try
            {
                var tokenResponse = await _fetchTokenTask;

                _cachedCredentialsProvider.Logger.LogDebug(
                    "Successfully fetched token. ExpiredAt: {ExpiredAt}, RefreshAt: {RefreshAt}",
                    tokenResponse.ExpiredAt, tokenResponse.RefreshAt
                );

                return _cachedCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenResponse, _cachedCredentialsProvider));
            }
            catch (Exception e)
            {
                _cachedCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return _cachedCredentialsProvider.UpdateState(this, new ErrorState(e, _cachedCredentialsProvider));
            }
        }

        public void Init() => _fetchTokenTask = _cachedCredentialsProvider.FetchToken();
    }

    private class BackgroundState : ITokenState
    {
        private readonly CachedCredentialsProvider _cachedCredentialsProvider;

        private volatile Task<TokenResponse> _fetchTokenTask = null!;

        public BackgroundState(TokenResponse tokenResponse,
            CachedCredentialsProvider cachedCredentialsProvider)
        {
            TokenResponse = tokenResponse;
            _cachedCredentialsProvider = cachedCredentialsProvider;
        }

        public TokenResponse TokenResponse { get; }

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            if (_fetchTokenTask.IsCanceled || _fetchTokenTask.IsFaulted)
            {
                _cachedCredentialsProvider.Logger.LogWarning(
                    "Fetching token task failed. Status: {Status}, Retrying login...",
                    _fetchTokenTask.IsCanceled ? "Canceled" : "Faulted"
                );

                return now >= TokenResponse.ExpiredAt
                    ? await _cachedCredentialsProvider
                        .UpdateState(this, new SyncState(_cachedCredentialsProvider))
                        .Validate(now)
                    : _cachedCredentialsProvider
                        .UpdateState(this, new BackgroundState(TokenResponse, _cachedCredentialsProvider));
            }

            if (_fetchTokenTask.IsCompleted)
            {
                return _cachedCredentialsProvider
                    .UpdateState(this, new ActiveState(await _fetchTokenTask, _cachedCredentialsProvider));
            }

            if (now < TokenResponse.ExpiredAt)
            {
                return this;
            }

            try
            {
                var tokenResponse = await _fetchTokenTask;

                _cachedCredentialsProvider.Logger.LogDebug(
                    "Successfully fetched token. ExpiredAt: {ExpiredAt}, RefreshAt: {RefreshAt}",
                    tokenResponse.ExpiredAt, tokenResponse.RefreshAt
                );

                return _cachedCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenResponse, _cachedCredentialsProvider));
            }
            catch (Exception e)
            {
                _cachedCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return _cachedCredentialsProvider.UpdateState(this,
                    new ErrorState(e, _cachedCredentialsProvider));
            }
        }

        public void Init() => _fetchTokenTask = _cachedCredentialsProvider.FetchToken();
    }

    private class ErrorState : ITokenState
    {
        private readonly Exception _exception;
        private readonly CachedCredentialsProvider _managerCredentialsProvider;

        public ErrorState(Exception exception, CachedCredentialsProvider managerCredentialsProvider)
        {
            _exception = exception;
            _managerCredentialsProvider = managerCredentialsProvider;
        }

        public TokenResponse TokenResponse => throw _exception;

        public ValueTask<ITokenState> Validate(DateTime now) => _managerCredentialsProvider
            .UpdateState(this, new SyncState(_managerCredentialsProvider))
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
