using System;
using System.Data.Common;
using EfCore.Ydb.Infrastructure;
using EfCore.Ydb.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EfCore.Ydb.Extensions;

public static class YdbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseEfYdb(
        this DbContextOptionsBuilder optionsBuilder,
        string? connectionString,
        Action<YdbDbContextOptionsBuilder>? efYdbOptionsAction = null
    )
    {
        var extension = GetOrCreateExtension(optionsBuilder).WithConnectionString(connectionString);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        efYdbOptionsAction?.Invoke(new YdbDbContextOptionsBuilder(optionsBuilder));
        return optionsBuilder;
    }

    public static DbContextOptionsBuilder UseEfYdb(
        this DbContextOptionsBuilder optionsBuilder,
        DbConnection connection,
        Action<YdbDbContextOptionsBuilder>? efYdbOptionsAction = null
    )
    {
        var extension = GetOrCreateExtension(optionsBuilder).WithConnection(connection);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(extension);

        ConfigureWarnings(optionsBuilder);

        efYdbOptionsAction?.Invoke(new YdbDbContextOptionsBuilder(optionsBuilder));
        return optionsBuilder;
    }

    // TODO: Right now there are no arguments for constructor, so probably it's ok
    private static YdbOptionsExtension GetOrCreateExtension(DbContextOptionsBuilder options)
        => options.Options.FindExtension<YdbOptionsExtension>() ?? new YdbOptionsExtension();

    private static void ConfigureWarnings(DbContextOptionsBuilder optionsBuilder)
    {
        var coreOptionsExtension = optionsBuilder.Options.FindExtension<CoreOptionsExtension>()
                                   ?? new CoreOptionsExtension();

        coreOptionsExtension = RelationalOptionsExtension.WithDefaultWarningConfiguration(coreOptionsExtension);
        ((IDbContextOptionsBuilderInfrastructure)optionsBuilder).AddOrUpdateExtension(coreOptionsExtension);
    }
}
