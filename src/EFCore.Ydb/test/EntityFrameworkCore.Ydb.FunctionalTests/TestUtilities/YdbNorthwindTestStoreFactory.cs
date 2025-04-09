using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

internal class YdbNorthwindTestStoreFactory : YdbTestStoreFactory
{
    private const string DatabaseName = "Northwind";

    public new static YdbNorthwindTestStoreFactory Instance { get; } = new();

    public override TestStore Create(string storeName) =>
        YdbTestStore.GetOrCreate(DatabaseName, scriptPath: $"{DatabaseName}.sql");

    public override TestStore GetOrCreate(string storeName) =>
        YdbTestStore.GetOrCreate(DatabaseName, scriptPath: $"{DatabaseName}.sql");
}
