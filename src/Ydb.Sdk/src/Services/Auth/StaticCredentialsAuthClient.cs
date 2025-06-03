using System.IdentityModel.Tokens.Jwt;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Ydb.Auth;
using Ydb.Auth.V1;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Client;
using Ydb.Sdk.Pool;
using Ydb.Sdk.Services.Operations;

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
            var loginResponse = await Login();
            var status = loginResponse.Status;

            if (status.IsSuccess)
            {
                return new TokenResponse(loginResponse.Result, new JwtSecurityToken(loginResponse.Result).ValidTo);
            }

            _logger.LogError("Login request get wrong status {Status}", status);

            var retryRule = _retrySettings.GetRetryRule(status.StatusCode);

            if (retryRule.Policy == RetryPolicy.None)
            {
                throw new StatusUnsuccessfulException(status);
            }

            if (++attempt >= _retrySettings.MaxAttempts)
            {
                throw new StatusUnsuccessfulException(status);
            }

            await Task.Delay(retryRule.BackoffSettings.CalcBackoff(attempt));
        }
    }

    private async Task<LoginResponse> Login()
    {
        var request = new LoginRequest
        {
            User = _config.User
        };

        if (_config.Password is not null)
        {
            request.Password = _config.Password;
        }

        try
        {
            using var channel = _grpcChannelFactory.CreateChannel(_config.Endpoint);

            var response = await new AuthService.AuthServiceClient(channel)
                .LoginAsync(request, new CallOptions(_config.GetCallMetadata));

            var status = response.Operation.TryUnpack(out LoginResult? resultProto);

            string? result = null;

            if (status.IsSuccess && resultProto is not null)
            {
                result = resultProto.Token;
            }

            return new LoginResponse(status, result);
        }
        catch (Driver.TransportException e)
        {
            return new LoginResponse(e.Status);
        }
    }

    private class LoginResponse : ResponseWithResultBase<string>
    {
        internal LoginResponse(Status status, string? token = null) : base(status, token)
        {
        }
    }
}
