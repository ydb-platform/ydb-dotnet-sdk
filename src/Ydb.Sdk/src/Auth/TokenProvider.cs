namespace Ydb.Sdk.Auth;

public class TokenProvider : ICredentialsProvider
{
    private readonly string _token;

    public TokenProvider(string token)
    {
        _token = token;
    }

    public ValueTask<string> GetAuthInfoAsync() => ValueTask.FromResult(_token);
}
