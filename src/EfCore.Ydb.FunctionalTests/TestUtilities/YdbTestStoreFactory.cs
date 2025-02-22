using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestStoreFactory : RelationalTestStoreFactory
{
    public static YdbTestStoreFactory Instance { get; } = new();
    
    private string? _scriptPath = null;
    private string? _additionalSql = null;
    private string? _connectionStringOptions = null;
    private readonly string? _connectionString;
    private bool _useConnectionString = false;

    public YdbTestStoreFactory(
        string? connectionString = null,
        string? additionalSql = null,
        string? connectionStringOptions = null,
        bool useConnectionString = false
    )
    {
        _connectionString = connectionString;
        _additionalSql = additionalSql;
        _connectionStringOptions = connectionStringOptions;
        _useConnectionString = useConnectionString;
    }

    public override TestStore Create(string storeName)
        => new YdbTestStore(storeName, _scriptPath, _additionalSql, _connectionStringOptions, shared: false, _useConnectionString);

    public override TestStore GetOrCreate(string storeName)
        => new YdbTestStore(storeName, _scriptPath, _additionalSql, _connectionStringOptions, shared: true, _useConnectionString);

    public override IServiceCollection AddProviderServices(IServiceCollection serviceCollection)
        => serviceCollection.AddEntityFrameworkYdb();
}
