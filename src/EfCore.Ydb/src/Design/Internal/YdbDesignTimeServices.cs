using EfCore.Ydb.Extensions;
using EfCore.Ydb.Scaffolding.Internal;
using Microsoft.EntityFrameworkCore.Design;
using Microsoft.EntityFrameworkCore.Scaffolding;
using Microsoft.Extensions.DependencyInjection;

namespace EfCore.Ydb.Design.Internal;

public class YdbDesignTimeServices : IDesignTimeServices
{
    public void ConfigureDesignTimeServices(IServiceCollection serviceCollection)
    {
        serviceCollection.AddEntityFrameworkYdb();

        new EntityFrameworkRelationalDesignServicesBuilder(serviceCollection)
            .TryAdd<IDatabaseModelFactory, YdbDatabaseModelFactory>()
            .TryAddCoreServices();
    }
}
