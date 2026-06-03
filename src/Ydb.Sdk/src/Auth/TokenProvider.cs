namespace Ydb.Sdk.Auth;

/// <summary>
/// Token-based authentication provider for YDB.
/// </summary>
/// <remarks>
/// TokenProvider provides authentication using a static access token.
/// This is suitable for scenarios where you have a pre-obtained token.
/// </remarks>
public class TokenProvider(string token) : ICredentialsProvider
{
    public ValueTask<string> GetAuthInfoAsync() => ValueTask.FromResult(token);
}
