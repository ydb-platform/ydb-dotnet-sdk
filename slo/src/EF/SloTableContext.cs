using EntityFrameworkCore.Ydb.Extensions;
using Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Prometheus;
using Ydb.Sdk;

namespace EF;

public class SloTableContext : SloTableContext<PooledDbContextFactory<TableDbContext>>
{
    protected override string Job => "EF";

    protected override PooledDbContextFactory<TableDbContext> CreateClient(Config config) =>
        new(new DbContextOptionsBuilder<TableDbContext>().UseYdb(config.ConnectionString).Options);

    protected override async Task Create(
        PooledDbContextFactory<TableDbContext> client,
        int operationTimeout
    )
    {
        await using var dbContext = await client.CreateDbContextAsync();
        await dbContext.Database.EnsureCreatedAsync();
        await dbContext.Database.ExecuteSqlRawAsync(SloTable.Options);
    }

    protected override async Task<(int, StatusCode)> Save(
        PooledDbContextFactory<TableDbContext> client,
        SloTable sloTable,
        int writeTimeout,
        Counter? errorsTotal = null)
    {
        await using var dbContext = await client.CreateDbContextAsync();
        dbContext.SloEntities.Add(sloTable);
        await dbContext.SaveChangesAsync();

        return (1, StatusCode.Success);
    }

    protected override async Task<(int, StatusCode, object?)> Select(
        PooledDbContextFactory<TableDbContext> client,
        (Guid Guid, int Id) select,
        int readTimeout,
        Counter? errorsTotal = null
    )
    {
        await using var dbContext = await client.CreateDbContextAsync();
        await dbContext.SloEntities.FindAsync(select.Guid, select.Id);

        return (0, StatusCode.Success, null);
    }

    protected override async Task<int> SelectCount(PooledDbContextFactory<TableDbContext> client)
    {
        await using var dbContext = await client.CreateDbContextAsync();

        return await dbContext.SloEntities.CountAsync();
    }
}