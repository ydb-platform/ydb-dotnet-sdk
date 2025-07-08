using System.Data;
using EntityFrameworkCore.Ydb.Extensions;
using Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Ydb.Sdk;
using Ydb.Sdk.Ado;

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
        int writeTimeout
    )
    {
        await using var context = await client.CreateDbContextAsync();
        var executeStrategy = context.Database.CreateExecutionStrategy();
        await executeStrategy.ExecuteAsync(async () =>
        {
            var dbContext = await client.CreateDbContextAsync();

            return await dbContext.Database.ExecuteSqlRawAsync(
                $"UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp) " +
                "VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp)",
                new YdbParameter
                {
                    DbType = DbType.Guid,
                    ParameterName = "Guid",
                    Value = sloTable.Guid
                },
                new YdbParameter
                {
                    DbType = DbType.Int32,
                    ParameterName = "Id",
                    Value = sloTable.Id
                },
                new YdbParameter
                {
                    DbType = DbType.String,
                    ParameterName = "PayloadStr",
                    Value = sloTable.PayloadStr
                },
                new YdbParameter
                {
                    DbType = DbType.Double,
                    ParameterName = "PayloadDouble",
                    Value = sloTable.PayloadDouble
                },
                new YdbParameter
                {
                    DbType = DbType.DateTime2,
                    ParameterName = "PayloadTimestamp",
                    Value = sloTable.PayloadTimestamp
                });
        });

        return (1, StatusCode.Success);
    }

    protected override async Task<(int, StatusCode, object?)> Select(
        PooledDbContextFactory<TableDbContext> client,
        (Guid Guid, int Id) select,
        int readTimeout
    )
    {
        await using var dbContext = await client.CreateDbContextAsync();
        return (0, StatusCode.Success,
            await dbContext.SloEntities.FirstOrDefaultAsync(table =>
                table.Guid == select.Guid && table.Id == select.Id));
    }

    protected override async Task<int> SelectCount(PooledDbContextFactory<TableDbContext> client)
    {
        await using var dbContext = await client.CreateDbContextAsync();
        return await dbContext.SloEntities.CountAsync();
    }
}