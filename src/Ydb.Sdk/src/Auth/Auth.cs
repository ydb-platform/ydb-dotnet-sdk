#if NETCOREAPP3_1
using System;
#endif

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
