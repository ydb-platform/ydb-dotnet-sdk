using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestStoreFactory(
    string? additionalSql = null,
    string? connectionStringOptions = null,
    bool useConnectionString = false
) : RelationalTestStoreFactory
{
    public static YdbTestStoreFactory Instance { get; } = new();

    private readonly string? _scriptPath = null;

    public override TestStore Create(string storeName) => 
        new YdbTestStore(storeName, _scriptPath, additionalSql, connectionStringOptions, shared: false, useConnectionString);

    public override TestStore GetOrCreate(string storeName)
        => new YdbTestStore(storeName, _scriptPath, additionalSql, connectionStringOptions, shared: true,
            useConnectionString);

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
        => serviceCollection.AddEntityFrameworkYdb();
}
