using Microsoft.Extensions.Logging;
using Ydb.Auth;
using Ydb.Auth.V1;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;
using Ydb.Sdk.Transport;

namespace Ydb.Sdk.Services.Auth;

internal class AuthClient
{
    private readonly DriverConfig _config;
    private readonly ILogger _logger;

    public AuthClient(DriverConfig config, ILogger logger)
    {
        _config = config;
        _logger = logger;
    }

    public async Task<LoginResponse> Login(string user, string? password, LoginSettings? settings = null)
    {
        settings ??= new LoginSettings();
        var request = new LoginRequest
        {
            OperationParams = settings.MakeOperationParams(),
            User = user
        };
        
        if (password is not null)
        {
            request.Password = password;
        }

        try
        {
            await using var transport = new FixedGrpcChannelTransport(_config, _logger);

            var response = await transport.UnaryCall(
                method: AuthService.LoginMethod,
                request: request,
                settings: settings
            );

            var status = response.Operation.TryUnpack(out LoginResult? resultProto);

            LoginResponse.ResultData? result = null;

            if (status.IsSuccess && resultProto is not null)
            {
                result = LoginResponse.ResultData.FromProto(resultProto);
            }

            return new LoginResponse(status, result);
        }
        catch (Driver.TransportException e)
        {
            return new LoginResponse(e.Status);
        }
    }
}

public class LoginSettings : OperationRequestSettings
{
}

public class LoginResponse : ResponseWithResultBase<LoginResponse.ResultData>
{
    internal LoginResponse(Status status, ResultData? result = null) : base(status, result)
    {
    }

    public class ResultData
    {
        public string Token { get; }

        private ResultData(string token)
        {
            Token = token;
        }


        internal static ResultData FromProto(LoginResult resultProto)
        {
            var token = resultProto.Token;
            return new ResultData(token);
        }
    }
}
