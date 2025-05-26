using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;

namespace EntityFrameworkCore.Ydb.Design.Internal;

public class YdbDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddEntityFrameworkYdb(useYdbExecutionStrategy: false);

        new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
            .TryAdd<IDatabaseModelFactory, YdbDatabaseModelFactory>()
            .TryAddCoreServices();
    }
}
