namespace Ydb.Sdk.Auth
{
    public class InvalidCredentialsException : Exception
    {
        public InvalidCredentialsException(string message)
            : base (message)
        {
        }
    }
}
