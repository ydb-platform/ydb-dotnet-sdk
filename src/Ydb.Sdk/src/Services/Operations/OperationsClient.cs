namespace Ydb.Sdk.Services.Operations;

public partial class OperationsClient
{
    private readonly Driver _driver;

    public OperationsClient(Driver driver)
    {
        _driver = driver;
    }
}