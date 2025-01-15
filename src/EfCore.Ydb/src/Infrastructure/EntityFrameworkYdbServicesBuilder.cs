using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EfCore.Ydb.Infrastructure;

public class EntityFrameworkYdbServicesBuilder : EntityFrameworkRelationalServicesBuilder
{
    private static readonly IDictionary<Type, ServiceCharacteristics> YdbServices
        = new Dictionary<Type, ServiceCharacteristics>
        {
            // TODO: Add items if required
        };

    public EntityFrameworkYdbServicesBuilder(IServiceCollection serviceCollection) : base(serviceCollection)
    {
    }

    protected override ServiceCharacteristics GetServiceCharacteristics(Type serviceType)
    {
        var contains = YdbServices.TryGetValue(serviceType, out var characteristics);
        return contains
            ? characteristics
            : base.GetServiceCharacteristics(serviceType);
    }
}
