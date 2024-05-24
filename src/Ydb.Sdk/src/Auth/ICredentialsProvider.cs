using Ydb.Sdk.Services.Auth;

namespace Ydb.Sdk.Auth;

public interface ICredentialsProvider
{
    string? GetAuthInfo();

    Task ProvideAuthClient(AuthClient authClient)
    {
        return Task.CompletedTask;
    }
}
