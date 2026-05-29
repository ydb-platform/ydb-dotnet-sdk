using System.IdentityModel.Tokens.Jwt;
using Microsoft.Extensions.Logging;
using Ydb.Auth;
using Ydb.Auth.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Auth;

internal class StaticCredentialsAuthClient : IAuthClient
{
    private readonly DriverConfig _config;
    private readonly GrpcChannelFactory _grpcChannelFactory;
    private readonly ILogger<StaticCredentialsAuthClient> _logger;

    // Login is idempotent, so the broader idempotent retry set is safe here.
    private readonly IRetryPolicy _retryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig
    {
        MaxAttempts = 5,
        EnableRetryIdempotence = true
    });

    internal StaticCredentialsAuthClient(
        DriverConfig config,
        GrpcChannelFactory grpcChannelFactory,
        ILoggerFactory loggerFactory
    )
    {
        _config = config;
        _grpcChannelFactory = grpcChannelFactory;
        _logger = loggerFactory.CreateLogger<StaticCredentialsAuthClient>();
    }

    public async Task<TokenResponse> FetchToken()
    {
        var attempt = 0;
        while (true)
        {
            try
            {
                var token = await Login();

                return new TokenResponse(token, new JwtSecurityToken(token).ValidTo);
            }
            catch (YdbException e)
            {
                _logger.LogError(e, "Login request get wrong status");

                if (_retryPolicy.GetNextDelay(e, attempt++) is not { } delay)
                {
                    throw;
                }

                await Task.Delay(delay);
            }
        }
    }

    private async Task<string> Login()
    {
        var request = new LoginRequest { User = _config.User };

        if (_config.Password is not null)
        {
            request.Password = _config.Password;
        }

        using var channel = _grpcChannelFactory.CreateChannel(_config.Endpoint);

        var response = await new AuthService.AuthServiceClient(channel)
            .LoginAsync(request, _config.GetCallMetadata);

        var operation = response.Operation;

        return operation.Status.IsNotSuccess()
            ? throw YdbException.FromServer(operation.Status, operation.Issues)
            : operation.Result.Unpack<LoginResult>().Token;
    }
}
