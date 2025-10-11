namespace Ydb.Sdk.Auth;

/// <summary>
/// Token-based authentication provider for YDB.
/// </summary>
/// <remarks>
/// TokenProvider provides authentication using a static access token.
/// This is suitable for scenarios where you have a pre-obtained token.
/// </remarks>
public class TokenProvider : ICredentialsProvider
{
    private readonly string _token;

    /// <summary>
    /// Initializes a new instance of the TokenProvider class.
    /// </summary>
    /// <param name="token">The access token to use for authentication.</param>
    public TokenProvider(string token)
    {
        _token = token;
    }

    public ValueTask<string> GetAuthInfoAsync() => ValueTask.FromResult(_token);
}
