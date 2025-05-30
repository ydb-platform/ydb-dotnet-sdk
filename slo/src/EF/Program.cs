// See https://aka.ms/new-console-template for more information

using EF;
using EntityFrameworkCore.Ydb.Extensions;
using Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

await Cli.Run((mode, config) =>
{
    return mode switch
    {
        CliMode.Create => new SloTableContext(new PooledDbContextFactory<TableDbContext>(
            new DbContextOptionsBuilder<TableDbContext>()
                .UseYdb(config.ConnectionString, builder => builder.DisableRetryOnFailure())
                .UseLoggerFactory(ISloContext.Factory)
                .Options)),
        CliMode.Run => new SloTableContext(new PooledDbContextFactory<TableDbContext>(
            new DbContextOptionsBuilder<TableDbContext>()
                .UseYdb(config.ConnectionString)
                .Options)),
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
    };
}, args);