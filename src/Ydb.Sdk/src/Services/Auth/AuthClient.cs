using Ydb.Sdk.Client;

namespace Ydb.Sdk.Services.Auth;

public partial class AuthClient : ClientBase
{
    public AuthClient(Driver driver) : base(driver)
    {
    }
}
