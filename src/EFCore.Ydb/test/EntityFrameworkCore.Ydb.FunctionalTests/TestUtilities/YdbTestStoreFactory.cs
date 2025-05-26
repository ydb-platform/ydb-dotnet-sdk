using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestStoreFactory(string? additionalSql = null, bool useYdbExecutionStrategy = true)
    : RelationalTestStoreFactory
{
    public static YdbTestStoreFactory Instance { get; } = new();

    private readonly string? _scriptPath = null;

    public override TestStore Create(string storeName) =>
        new YdbTestStore(storeName, _scriptPath, additionalSql);

    public override TestStore GetOrCreate(string storeName)
        => new YdbTestStore(storeName, _scriptPath, additionalSql);

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
        => serviceCollection.AddEntityFrameworkYdb(useYdbExecutionStrategy);
}
