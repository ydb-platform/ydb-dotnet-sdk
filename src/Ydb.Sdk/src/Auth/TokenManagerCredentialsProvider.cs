using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Auth;

public class TokenManagerCredentialsProvider : ICredentialsProvider
{
    private readonly IClock _clock;
    private readonly IAuthClient _authClient;

    private ILogger<TokenManagerCredentialsProvider> Logger { get; }

    private volatile ITokenState _tokenState;

    public TokenManagerCredentialsProvider(
        IAuthClient authClient,
        ILoggerFactory? loggerFactory = null
    )
    {
        _clock = new SystemClock();
        _authClient = authClient;
        _tokenState = new SyncState(this);
        _tokenState.Init();

        loggerFactory ??= new NullLoggerFactory();
        Logger = loggerFactory.CreateLogger<TokenManagerCredentialsProvider>();
    }

    internal TokenManagerCredentialsProvider(IAuthClient authClient, IClock clock) : this(authClient)
    {
        _clock = clock;
    }

    public async ValueTask<string> GetAuthInfoAsync() =>
        (await _tokenState.Validate(_clock.UtcNow)).TokenResponse.Token;

    private ValueTask<TokenResponse> FetchToken() => _authClient.FetchToken();

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

    private class ActiveState : ITokenState
    {
        private readonly TokenManagerCredentialsProvider _tokenManagerCredentialsProvider;

        public ActiveState(TokenResponse tokenResponse, TokenManagerCredentialsProvider tokenManagerCredentialsProvider)
        {
            TokenResponse = tokenResponse;
            _tokenManagerCredentialsProvider = tokenManagerCredentialsProvider;
        }

        public TokenResponse TokenResponse { get; }

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            if (now < TokenResponse.ExpiredAt)
            {
                return now >= TokenResponse.RefreshAt
                    ? _tokenManagerCredentialsProvider.UpdateState(
                        this,
                        new BackgroundState(TokenResponse, _tokenManagerCredentialsProvider)
                    )
                    : this;
            }

            _tokenManagerCredentialsProvider.Logger.LogWarning(
                "Token has expired. ExpiredAt: {ExpiredAt}, CurrentTime: {CurrentTime}. " +
                "Switching to synchronous state to fetch a new token",
                TokenResponse.ExpiredAt, now
            );

            return await _tokenManagerCredentialsProvider
                .UpdateState(this, new SyncState(_tokenManagerCredentialsProvider))
                .Validate(now);
        }
    }

    private class SyncState : ITokenState
    {
        private readonly TokenManagerCredentialsProvider _tokenManagerCredentialsProvider;

        private volatile Task<TokenResponse> _fetchTokenTask = null!;

        public SyncState(TokenManagerCredentialsProvider tokenManagerCredentialsProvider)
        {
            _tokenManagerCredentialsProvider = tokenManagerCredentialsProvider;
        }

        public TokenResponse TokenResponse =>
            throw new InvalidOperationException("Get token for unfinished sync state");

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            try
            {
                var tokenResponse = await _fetchTokenTask;

                _tokenManagerCredentialsProvider.Logger.LogDebug(
                    "Successfully fetched token at {Timestamp}. ExpiredAt: {ExpiredAt}, RefreshAt: {RefreshAt}",
                    DateTime.Now, tokenResponse.ExpiredAt, tokenResponse.RefreshAt
                );

                return _tokenManagerCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenResponse, _tokenManagerCredentialsProvider));
            }
            catch (Exception e)
            {
                _tokenManagerCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return _tokenManagerCredentialsProvider.UpdateState(this,
                    new ErrorState(e, _tokenManagerCredentialsProvider));
            }
        }

        public void Init() => _fetchTokenTask = _tokenManagerCredentialsProvider.FetchToken().AsTask();
    }

    private class BackgroundState : ITokenState
    {
        private readonly TokenManagerCredentialsProvider _tokenManagerCredentialsProvider;

        private volatile Task<TokenResponse> _fetchTokenTask = null!;

        public BackgroundState(TokenResponse tokenResponse,
            TokenManagerCredentialsProvider tokenManagerCredentialsProvider)
        {
            TokenResponse = tokenResponse;
            _tokenManagerCredentialsProvider = tokenManagerCredentialsProvider;
        }

        public TokenResponse TokenResponse { get; }

        public async ValueTask<ITokenState> Validate(DateTime now)
        {
            if (_fetchTokenTask.IsCanceled || _fetchTokenTask.IsFaulted)
            {
                _tokenManagerCredentialsProvider.Logger.LogWarning(
                    "Fetching token task failed. Status: {Status}, Retrying login...",
                    _fetchTokenTask.IsCanceled ? "Canceled" : "Faulted"
                );

                return now >= TokenResponse.ExpiredAt
                    ? _tokenManagerCredentialsProvider
                        .UpdateState(this, new SyncState(_tokenManagerCredentialsProvider))
                    : _tokenManagerCredentialsProvider
                        .UpdateState(this, new BackgroundState(TokenResponse, _tokenManagerCredentialsProvider));
            }

            if (_fetchTokenTask.IsCompleted)
            {
                return _tokenManagerCredentialsProvider
                    .UpdateState(this, new ActiveState(await _fetchTokenTask, _tokenManagerCredentialsProvider));
            }

            if (now < TokenResponse.ExpiredAt)
            {
                return this;
            }

            try
            {
                var tokenResponse = await _fetchTokenTask;

                _tokenManagerCredentialsProvider.Logger.LogDebug(
                    "Successfully fetched token. ExpiredAt: {ExpiredAt}, RefreshAt: {RefreshAt}",
                    tokenResponse.ExpiredAt, tokenResponse.RefreshAt
                );

                return _tokenManagerCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenResponse, _tokenManagerCredentialsProvider));
            }
            catch (Exception e)
            {
                _tokenManagerCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return _tokenManagerCredentialsProvider.UpdateState(this,
                    new ErrorState(e, _tokenManagerCredentialsProvider));
            }
        }

        public void Init() => _fetchTokenTask = _tokenManagerCredentialsProvider.FetchToken().AsTask();
    }

    private class ErrorState : ITokenState
    {
        private readonly Exception _exception;
        private readonly TokenManagerCredentialsProvider _managerCredentialsProvider;

        public ErrorState(Exception exception, TokenManagerCredentialsProvider managerCredentialsProvider)
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
