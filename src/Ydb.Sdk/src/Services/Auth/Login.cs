using Ydb.Auth;
using Ydb.Auth.V1;
using Ydb.Sdk.Client;
using Ydb.Sdk.Services.Operations;

namespace Ydb.Sdk.Services.Auth;

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

        internal ResultData(string token)
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

public partial class AuthClient
{
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
            var response = await _driver.UnaryCall(
                method: AuthService.LoginMethod,
                request: request,
                settings: settings
            );

            var status = response.Data.Operation.TryUnpack(out LoginResult? resultProto);

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
