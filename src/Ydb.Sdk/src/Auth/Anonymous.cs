namespace Ydb.Sdk.Auth;

public class AnonymousProvider : ICredentialsProvider
{
    public string? GetAuthInfo()
    {
        return null;
    }
}