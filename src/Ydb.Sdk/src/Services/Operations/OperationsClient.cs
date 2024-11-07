namespace Ydb.Sdk.Services.Operations;

public partial class OperationsClient
{
    private readonly IDriver _driver;

    public OperationsClient(Driver driver)
    {
        _driver = driver;
    }

    internal OperationsClient(IDriver driver)
    {
        _driver = driver;
    }
}
