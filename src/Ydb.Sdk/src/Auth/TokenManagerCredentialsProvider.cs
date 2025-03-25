using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]

namespace Ydb.Sdk.Auth;

public class TokenManagerCredentialsProvider : ICredentialsProvider
{
    private readonly IClock _clock;
    private readonly IAuthClient _authClient;

    private ILogger<TokenManagerCredentialsProvider> Logger { get; }

    private volatile IHolderState _holderState;

    public TokenManagerCredentialsProvider(
        IAuthClient authClient,
        ILoggerFactory? loggerFactory = null
    )
    {
        _clock = new SystemClock();
        _authClient = authClient;
        _holderState = new SyncState(this);
        _holderState.Init();

        loggerFactory ??= new NullLoggerFactory();
        Logger = loggerFactory.CreateLogger<TokenManagerCredentialsProvider>();
    }

    internal TokenManagerCredentialsProvider(IAuthClient authClient, IClock clock) : this(authClient)
    {
        _clock = clock;
    }

    public async ValueTask<string> GetAuthInfoAsync() =>
        (await _holderState.Validate(_clock.UtcNow)).TokenResponse.Token;

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
        private readonly TokenManagerCredentialsProvider _tokenManagerCredentialsProvider;

        public ActiveState(TokenResponse tokenResponse, TokenManagerCredentialsProvider tokenManagerCredentialsProvider)
        {
            TokenResponse = tokenResponse;
            _tokenManagerCredentialsProvider = tokenManagerCredentialsProvider;
        }

        public TokenResponse TokenResponse { get; }

        public async ValueTask<IHolderState> Validate(DateTime now)
        {
            if (now >= TokenResponse.ExpiresAt)
            {
                return await _tokenManagerCredentialsProvider
                    .UpdateState(this, new SyncState(_tokenManagerCredentialsProvider))
                    .Validate(now);
            }

            return now >= TokenResponse.RefreshAt
                ? _tokenManagerCredentialsProvider.UpdateState(
                    this,
                    new BackgroundState(TokenResponse, _tokenManagerCredentialsProvider)
                )
                : this;
        }
    }

    private class SyncState : IHolderState
    {
        private readonly TokenManagerCredentialsProvider _tokenManagerCredentialsProvider;

        private volatile Task<TokenResponse> _fetchTokenTask = null!;

        public SyncState(TokenManagerCredentialsProvider tokenManagerCredentialsProvider)
        {
            _tokenManagerCredentialsProvider = tokenManagerCredentialsProvider;
        }

        public TokenResponse TokenResponse =>
            throw new InvalidOperationException("Get token for unfinished sync state");

        public async ValueTask<IHolderState> Validate(DateTime now)
        {
            try
            {
                var tokenHolder = await _fetchTokenTask;

                return _tokenManagerCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenHolder, _tokenManagerCredentialsProvider));
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

    private class BackgroundState : IHolderState
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

        public async ValueTask<IHolderState> Validate(DateTime now)
        {
            if (_fetchTokenTask.IsCanceled || _fetchTokenTask.IsFaulted)
            {
                return now > TokenResponse.ExpiresAt
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

            if (now < TokenResponse.ExpiresAt)
            {
                return this;
            }

            try
            {
                var tokenHolder = await _fetchTokenTask;

                return _tokenManagerCredentialsProvider.UpdateState(this,
                    new ActiveState(tokenHolder, _tokenManagerCredentialsProvider));
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

    private class ErrorState : IHolderState
    {
        private readonly Exception _exception;
        private readonly TokenManagerCredentialsProvider _managerCredentialsProvider;

        public ErrorState(Exception exception, TokenManagerCredentialsProvider managerCredentialsProvider)
        {
            _exception = exception;
            _managerCredentialsProvider = managerCredentialsProvider;
        }

        public TokenResponse TokenResponse => throw _exception;

        public ValueTask<IHolderState> Validate(DateTime now) => _managerCredentialsProvider
            .UpdateState(this, new SyncState(_managerCredentialsProvider))
            .Validate(now);
    }
}

public class TokenResponse
{
    private const double RefreshInterval = 0.5;

    public TokenResponse(string token, DateTime expiresAt, DateTime? refreshAt = null)
    {
        var now = DateTime.UtcNow;

        Token = token;
        ExpiresAt = expiresAt.ToUniversalTime();
        RefreshAt = refreshAt?.ToUniversalTime() ?? now + (ExpiresAt - now) * RefreshInterval;
    }

    internal string Token { get; }
    internal DateTime ExpiresAt { get; }
    internal DateTime RefreshAt { get; }
}

internal interface IClock
{
    DateTime UtcNow { get; }
}

internal class SystemClock : IClock
{
    public DateTime UtcNow => DateTime.UtcNow;
}
