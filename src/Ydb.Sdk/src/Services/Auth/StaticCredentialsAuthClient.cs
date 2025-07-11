using System.IdentityModel.Tokens.Jwt;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Ydb.Auth;
using Ydb.Auth.V1;
using Ydb.Sdk.Ado.Pool;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Auth;

namespace Ydb.Sdk.Services.Auth;

internal class StaticCredentialsAuthClient : IAuthClient
{
    private readonly DriverConfig _config;
    private readonly GrpcChannelFactory _grpcChannelFactory;
    private readonly ILogger<StaticCredentialsAuthClient> _logger;

    private readonly RetrySettings _retrySettings = new(5);

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
        uint attempt = 0;
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

                var retryRule = _retrySettings.GetRetryRule(e.Code);

                if (retryRule.Policy == RetryPolicy.None || ++attempt >= _retrySettings.MaxAttempts)
                {
                    throw;
                }

                await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt));
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
            .LoginAsync(request, new CallOptions(_config.GetCallMetadata));

        var operation = response.Operation;
        if (operation.Status.IsNotSuccess())
        {
            throw YdbException.FromServer(operation.Status, operation.Issues);
        }

        return operation.Result.Unpack<LoginResult>().Token;
    }
}
