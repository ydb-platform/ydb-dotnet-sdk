using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Auth;

public partial class AuthClient
{
    private readonly Driver _driver;
    public AuthClient(Driver driver)
    {
        _driver = driver;
    }
}
