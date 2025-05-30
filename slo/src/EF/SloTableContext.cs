using Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Prometheus;
using Ydb.Sdk;

namespace EF;

public class SloTableContext(PooledDbContextFactory<TableDbContext> client) : SloTableContextBase
{
    protected override string Job => "EF";

    protected override async Task Create(
        int operationTimeout
    )
    {
        await using var dbContext = await client.CreateDbContextAsync();
        await dbContext.Database.EnsureCreatedAsync();
        await dbContext.Database.ExecuteSqlRawAsync(SloTable.Options);
    }

    protected override async Task<(int, StatusCode)> Save(
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
        dynamic select,
        int readTimeout,
        Counter? errorsTotal = null
    )
    {
        await using var dbContext = await client.CreateDbContextAsync();
        await dbContext.SloEntities.FindAsync(select.Guid, select.Id);

        return (0, StatusCode.Success, null);
    }

    protected override async Task<int> SelectCount(string sql)
    {
        await using var dbContext = await client.CreateDbContextAsync();

        return await dbContext.Database.SqlQueryRaw<int>($"SELECT COUNT(*) FROM {SloTable.Name}").SingleAsync();
    }
}