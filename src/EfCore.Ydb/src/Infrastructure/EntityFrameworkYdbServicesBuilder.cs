using System;
using System.Collections.Generic;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EfCore.Ydb.Infrastructure;

public class EntityFrameworkYdbServicesBuilder(IServiceCollection serviceCollection)
    : EntityFrameworkRelationalServicesBuilder(serviceCollection)
{
    private static readonly IDictionary<Type, ServiceCharacteristics> YdbServices
        = new Dictionary<Type, ServiceCharacteristics>
        {
            // TODO: Add items if required
        };

    protected override ServiceCharacteristics GetServiceCharacteristics(Type serviceType)
    {
        var contains = YdbServices.TryGetValue(serviceType, out var characteristics);
        return contains
            ? characteristics
            : base.GetServiceCharacteristics(serviceType);
    }
}
