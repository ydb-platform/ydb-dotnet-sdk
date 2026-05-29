using System.IdentityModel.Tokens.Jwt;
using Ydb.Auth;
using Ydb.Auth.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.Internal;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Pool;

namespace Ydb.Sdk.Auth;

internal class StaticCredentialsAuthClient : IAuthClient
{
    private static readonly YdbRetryPolicyExecutor RetryPolicyExecutor =
        new(YdbRetryPolicy.IdempotenceDefault, "ydb.Login");

    private readonly DriverConfig _config;
    private readonly GrpcChannelFactory _grpcChannelFactory;

    internal StaticCredentialsAuthClient(DriverConfig config, GrpcChannelFactory grpcChannelFactory)
    {
        _config = config;
        _grpcChannelFactory = grpcChannelFactory;
    }

    public Task<TokenResponse> FetchToken() => RetryPolicyExecutor.ExecuteAsync(async _ =>
    {
        var token = await Login();

        return new TokenResponse(token, new JwtSecurityToken(token).ValidTo);
    });

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
