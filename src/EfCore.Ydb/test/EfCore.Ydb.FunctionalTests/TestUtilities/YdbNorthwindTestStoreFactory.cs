using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

public class YdbNorthwindTestStoreFactory : YdbTestStoreFactory
{
    private const string DatabaseName = "Northwind";

    public new static YdbNorthwindTestStoreFactory Instance { get; } = new();

    public override TestStore GetOrCreate(string storeName)
        => YdbTestStore.GetOrCreate(DatabaseName, scriptPath: $"{DatabaseName}.sql");
}
