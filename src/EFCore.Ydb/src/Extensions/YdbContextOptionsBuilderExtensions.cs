using System;
using System.Data.Common;
using EntityFrameworkCore.Ydb.Infrastructure;
using EntityFrameworkCore.Ydb.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EntityFrameworkCore.Ydb.Extensions;

public static class YdbContextOptionsBuilderExtensions
{
    public static DbContextOptionsBuilder UseYdb(
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

    public static DbContextOptionsBuilder UseYdb(
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

    public static DbContextOptionsBuilder<TContext> UseYdb<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        string? connectionString,
        Action<YdbDbContextOptionsBuilder>? ydbOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseYdb(
            (DbContextOptionsBuilder)optionsBuilder, connectionString, ydbOptionsAction);

    public static DbContextOptionsBuilder<TContext> UseYdb<TContext>(
        this DbContextOptionsBuilder<TContext> optionsBuilder,
        DbConnection connection,
        Action<YdbDbContextOptionsBuilder>? ydbOptionsAction = null)
        where TContext : DbContext
        => (DbContextOptionsBuilder<TContext>)UseYdb(
            (DbContextOptionsBuilder)optionsBuilder, connection, ydbOptionsAction);

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
