using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Ydb.Sdk.Auth;

public class TokenCredentialsProvider : ICredentialsProvider
{
    private readonly IAuthClient _authClient;
    private ILogger<TokenCredentialsProvider> Logger { get; }

    private volatile IHolderState _holderState;

    public TokenCredentialsProvider(
        IAuthClient authClient,
        ILoggerFactory? loggerFactory = null
    )
    {
        _authClient = authClient;
        _holderState = new SyncState(this);
        _holderState.Init();

        loggerFactory ??= new NullLoggerFactory();
        Logger = loggerFactory.CreateLogger<TokenCredentialsProvider>();
    }

    public async ValueTask<string> GetAuthInfoAsync() =>
        (await _holderState.Validate(DateTime.UtcNow)).TokenResponse.Token;

    private ValueTask<TokenResponse> FetchToken() => _authClient.FetchToken();

    private IHolderState UpdateState(IHolderState current, IHolderState next)
    {
        if (Interlocked.CompareExchange(ref _holderState, next, current) == current)
        {
            next.Init();
        }

        return _holderState;
    }

    private interface IHolderState
    {
        TokenResponse TokenResponse { get; }

        ValueTask<IHolderState> Validate(DateTime now);

        void Init()
        {
        }
    }

    private class ActiveState : IHolderState
    {
        private readonly TokenCredentialsProvider _tokenCredentialsProvider;

        public ActiveState(TokenResponse tokenResponse, TokenCredentialsProvider tokenCredentialsProvider)
        {
            TokenResponse = tokenResponse;
            _tokenCredentialsProvider = tokenCredentialsProvider;
        }

        public TokenResponse TokenResponse { get; }

        public async ValueTask<IHolderState> Validate(DateTime now)
        {
            if (now >= TokenResponse.ExpiresAt)
            {
                return await _tokenCredentialsProvider
                    .UpdateState(this, new SyncState(_tokenCredentialsProvider))
                    .Validate(now);
            }

            return now >= TokenResponse.RefreshAt
                ? _tokenCredentialsProvider.UpdateState(
                    this,
                    new BackgroundState(TokenResponse, _tokenCredentialsProvider)
                )
                : this;
        }
    }

    private class SyncState : IHolderState
    {
        private readonly TokenCredentialsProvider _tokenCredentialsProvider;

        private volatile Task<TokenResponse> _fetchTokenTask = null!;

        public SyncState(TokenCredentialsProvider tokenCredentialsProvider)
        {
            _tokenCredentialsProvider = tokenCredentialsProvider;
        }

        public TokenResponse TokenResponse =>
            throw new InvalidOperationException("Get token for unfinished sync state");

        public async ValueTask<IHolderState> Validate(DateTime now)
        {
            try
            {
                var tokenHolder = await _fetchTokenTask;

                return _tokenCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenHolder, _tokenCredentialsProvider));
            }
            catch (Exception e)
            {
                _tokenCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return new ErrorState(e, _tokenCredentialsProvider);
            }
        }

        public void Init() => _fetchTokenTask = _tokenCredentialsProvider.FetchToken().AsTask();
    }

    private class BackgroundState : IHolderState
    {
        private readonly TokenCredentialsProvider _tokenCredentialsProvider;

        private volatile Task<TokenResponse> _fetchTokenTask = null!;

        public BackgroundState(TokenResponse tokenResponse, TokenCredentialsProvider tokenCredentialsProvider)
        {
            TokenResponse = tokenResponse;
            _tokenCredentialsProvider = tokenCredentialsProvider;
        }

        public TokenResponse TokenResponse { get; }

        public async ValueTask<IHolderState> Validate(DateTime now)
        {
            if (_fetchTokenTask.IsCanceled || _fetchTokenTask.IsFaulted)
            {
                return now > TokenResponse.ExpiresAt
                    ? _tokenCredentialsProvider
                        .UpdateState(this, new SyncState(_tokenCredentialsProvider))
                    : _tokenCredentialsProvider
                        .UpdateState(this, new BackgroundState(TokenResponse, _tokenCredentialsProvider));
            }

            if (_fetchTokenTask.IsCompleted)
            {
                return _tokenCredentialsProvider
                    .UpdateState(this, new ActiveState(await _fetchTokenTask, _tokenCredentialsProvider));
            }

            if (now < TokenResponse.ExpiresAt)
            {
                return this;
            }

            try
            {
                var tokenHolder = await _fetchTokenTask;

                return new ActiveState(tokenHolder, _tokenCredentialsProvider);
            }
            catch (Exception e)
            {
                _tokenCredentialsProvider.Logger.LogCritical(e, "Error on authentication token update");

                return new ErrorState(e, _tokenCredentialsProvider);
            }
        }

        public void Init() => _fetchTokenTask = _tokenCredentialsProvider.FetchToken().AsTask();
    }

    private class ErrorState : IHolderState
    {
        private readonly Exception _exception;
        private readonly TokenCredentialsProvider _credentialsProvider;

        public ErrorState(Exception exception, TokenCredentialsProvider credentialsProvider)
        {
            _exception = exception;
            _credentialsProvider = credentialsProvider;
        }

        public TokenResponse TokenResponse => throw _exception;

        public ValueTask<IHolderState> Validate(DateTime now) => _credentialsProvider
            .UpdateState(this, new SyncState(_credentialsProvider))
            .Validate(now);
    }
}

public class TokenResponse
{
    private const double RefreshInterval = 0.5;

    public TokenResponse(string token, DateTime expiresAt)
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
            RefreshAt = now + (expiresAt - now) * RefreshInterval;

            if (RefreshAt < now)
            {
                RefreshAt = expiresAt;
            }
        }
    }

    internal string Token { get; }
    internal DateTime ExpiresAt { get; }
    internal DateTime RefreshAt { get; }
}
