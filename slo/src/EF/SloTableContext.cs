using System.Data;
using EntityFrameworkCore.Ydb.Extensions;
using Internal;
using Microsoft.EntityFrameworkCore;
using Ydb.Sdk;
using Ydb.Sdk.Ado;

namespace EF;

public class SloTableContext : SloTableContext<Func<TableDbContext>>
{
    protected override string Job => "EF";

    protected override Func<TableDbContext> CreateClient(Config config) =>
        () => new TableDbContext(new DbContextOptionsBuilder<TableDbContext>().UseYdb(config.ConnectionString).Options);

    protected override async Task Create(
        Func<TableDbContext> client,
        int operationTimeout
    )
    {
        await using var dbContext = client();
        await dbContext.Database.EnsureCreatedAsync();
        await dbContext.Database.ExecuteSqlRawAsync(SloTable.Options);
    }

    protected override async Task<(int, StatusCode)> Save(
        Func<TableDbContext> client,
        SloTable sloTable,
        int writeTimeout
    )
    {
        await using var dbContext = client();
        var executeStrategy = dbContext.Database.CreateExecutionStrategy();
        await executeStrategy.ExecuteAsync(async () => await dbContext.Database.ExecuteSqlRawAsync(
            $"UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp) " +
            "VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp)",
            new YdbParameter
            {
                DbType = DbType.String,
                ParameterName = "Guid",
                Value = sloTable.Guid.ToString()
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
            }));

        return (1, StatusCode.Success);
    }

    protected override async Task<(int, StatusCode, object?)> Select(
        Func<TableDbContext> client,
        (Guid Guid, int Id) select,
        int readTimeout
    )
    {
        await using var dbContext = client();
        await dbContext.SloEntities
            .SingleAsync(table => table.Guid == select.Guid && table.Id == select.Id);

        return (0, StatusCode.Success, null);
    }

    protected override async Task<int> SelectCount(Func<TableDbContext> client)
    {
        await using var dbContext = client();
        var count = await dbContext.SloEntities.CountAsync();

        return count;
    }
}