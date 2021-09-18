namespace Ydb.Sdk.Auth
{
    public class TokenProvider : ICredentialsProvider
    {
        private string _token;

        public TokenProvider(string token)
        {
            _token = token;
        }

        public string? GetAuthInfo()
        {
            return _token;
        }
    }
}
