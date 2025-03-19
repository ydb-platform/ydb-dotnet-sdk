using Ydb.Sdk.Services.Auth;

namespace Ydb.Sdk.Auth;

public interface ICredentialsProvider
{
    // For removal in 1.*
    string? GetAuthInfo();

    ValueTask<string?> GetAuthInfoAsync() => ValueTask.FromResult(GetAuthInfo());

    Task ProvideAuthClient(AuthClient authClient) => Task.CompletedTask;
}
