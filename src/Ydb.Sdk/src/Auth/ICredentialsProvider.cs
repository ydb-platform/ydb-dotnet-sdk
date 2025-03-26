namespace Ydb.Sdk.Auth;

public interface ICredentialsProvider
{
    ValueTask<string> GetAuthInfoAsync();
}

public interface IAuthClient
{
    Task<TokenResponse> FetchToken();
}
