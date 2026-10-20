using Internal;
using LinqToDB;
using LinqToDB.Async;
using LinqToDB.Data;
using LinqToDB.DataProvider.Ydb;
using LinqToDB.Mapping;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace Linq2db;

public sealed class SloTableContext : SloTableContext<SloTableContext.Linq2dbClient>
{
    protected override string Job => "Linq2db";

    public sealed class Linq2dbClient(string connectionString)
    {
        private readonly YdbDataSource _dataSource = new(
            new YdbConnectionStringBuilder(connectionString) { LoggerFactory = ISloContext.Factory });

        public async Task<DataConnection> OpenAsync() => new(new DataOptions().UseConnection(
            YdbTools.GetDataProvider(),
            await _dataSource.OpenRetryableConnectionAsync(new YdbRetryPolicy(ISloContext.RetryPolicyConfig)), true));
    }

    protected override Linq2dbClient CreateClient(SloConfig config) => new(config.ConnectionString);

    protected override async Task Create(Linq2dbClient client, int operationTimeout)
    {
        await using var db = await client.OpenAsync();
        db.CommandTimeout = operationTimeout;

        await db.ExecuteAsync($"""
                                   CREATE TABLE IF NOT EXISTS `{SloTable.Name}` (
                                       Guid             Uuid,
                                       Id               Int32,
                                       PayloadStr       Text,
                                       PayloadDouble    Double,
                                       PayloadTimestamp Timestamp,
                                       PRIMARY KEY (Guid, Id)
                                   )
                               """);

        await db.ExecuteAsync(SloTable.Options);
    }

    protected override async Task<int> Save(Linq2dbClient client, SloTable sloTable, int writeTimeout)
    {
        await using var db = await client.OpenAsync();
        db.CommandTimeout = writeTimeout;

        var affected = await db.ExecuteAsync(
            $"""
             UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
             VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp);
             """,
            new DataParameter("Guid", sloTable.Guid, DataType.Guid),
            new DataParameter("Id", sloTable.Id, DataType.Int32),
            new DataParameter("PayloadStr", sloTable.PayloadStr, DataType.NVarChar),
            new DataParameter("PayloadDouble", sloTable.PayloadDouble, DataType.Double),
            new DataParameter("PayloadTimestamp", sloTable.PayloadTimestamp, DataType.DateTime2)
        );

        return affected > 0 ? affected : 1;
    }

    protected override async Task<SloTable?> Select(Linq2dbClient client, (Guid Guid, int Id) select, int readTimeout)
    {
        await using var db = await client.OpenAsync();
        db.CommandTimeout = readTimeout;

        var row = await db.GetTable<SloRow>()
            .SingleOrDefaultAsync(sloRow => sloRow.Guid == select.Guid && sloRow.Id == select.Id);

        return row == null
            ? null
            : new SloTable
            {
                Guid = row.Guid,
                Id = row.Id,
                PayloadStr = row.PayloadStr ?? throw new InvalidOperationException("KV payload is null"),
                PayloadDouble = row.PayloadDouble,
                PayloadTimestamp = row.PayloadTimestamp
            };
    }

    [Table(SloTable.Name)]
    private sealed class SloRow(Guid guid, int id, string? payloadStr, double payloadDouble, DateTime payloadTimestamp)
    {
        [Column] public Guid Guid { get; } = guid;
        [Column] public int Id { get; } = id;

        // ReSharper disable once UnusedMember.Local
        [Column] public string? PayloadStr { get; } = payloadStr;

        // ReSharper disable once UnusedMember.Local
        [Column] public double PayloadDouble { get; } = payloadDouble;

        // ReSharper disable once UnusedMember.Local
        [Column] public DateTime PayloadTimestamp { get; } = payloadTimestamp;
    }
}
