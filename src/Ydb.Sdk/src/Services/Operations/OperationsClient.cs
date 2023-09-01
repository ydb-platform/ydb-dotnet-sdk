namespace Ydb.Sdk.Operations;

public partial class OperationsClient
{
    private readonly Driver _driver;

    public OperationsClient(Driver driver)
    {
        _driver = driver;
    }
}