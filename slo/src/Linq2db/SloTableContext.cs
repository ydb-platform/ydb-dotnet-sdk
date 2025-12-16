using Internal;
using LinqToDB;
using LinqToDB.Async;
using LinqToDB.Data;
using LinqToDB.Mapping;

namespace Linq2db;

public sealed class SloTableContext : SloTableContext<SloTableContext.Linq2dbClient>
{
    protected override string Job => "Linq2db";

    static SloTableContext()
    {
        YdbSdkRetryPolicyRegistration.UseGloballyWithIdempotence();
    }

    public sealed class Linq2dbClient(string connectionString)
    {
        public DataConnection Open()
            => new(new DataOptions().UseConnectionString("YDB", connectionString));
    }

    protected override Linq2dbClient CreateClient(Config config) => new(config.ConnectionString);

    protected override async Task Create(Linq2dbClient client, int operationTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = operationTimeout;

        await db.ExecuteAsync($@"
            CREATE TABLE `{SloTable.Name}` (
                Guid             Uuid,
                Id               Int32,
                PayloadStr       Text,
                PayloadDouble    Double,
                PayloadTimestamp Timestamp,
                PRIMARY KEY (Guid, Id)
            )");

        await db.ExecuteAsync(SloTable.Options);
    }

    protected override async Task<int> Save(Linq2dbClient client, SloTable sloTable, int writeTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = writeTimeout;

        var sql = $@"
UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp);";

        var affected = await db.ExecuteAsync(
            sql,
            new DataParameter("Guid", sloTable.Guid, DataType.Guid),
            new DataParameter("Id", sloTable.Id, DataType.Int32),
            new DataParameter("PayloadStr", sloTable.PayloadStr, DataType.NVarChar),
            new DataParameter("PayloadDouble", sloTable.PayloadDouble, DataType.Double),
            new DataParameter("PayloadTimestamp", sloTable.PayloadTimestamp, DataType.DateTime2)
        );

        return affected > 0 ? affected : 1;
    }

    protected override async Task<object?> Select(Linq2dbClient client, (Guid Guid, int Id) select, int readTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = readTimeout;

        var t = db.GetTable<SloRow>();

        var row = await t
            .Where(r => r.Guid == select.Guid && r.Id == select.Id)
            .Select(r => new
            {
                r.Guid,
                r.Id,
                r.PayloadStr,
                r.PayloadDouble,
                r.PayloadTimestamp
            })
            .FirstOrDefaultAsync();

        return row;
    }

    protected override async Task<int> SelectCount(Linq2dbClient client)
    {
        await using var db = client.Open();
        return await db.GetTable<SloRow>().CountAsync();
    }

    [Table(SloTable.Name)]
    private sealed class SloRow(Guid guid, int id, string? payloadStr, double payloadDouble, DateTime payloadTimestamp)
    {
        [Column] public Guid Guid { get; } = guid;
        [Column] public int Id { get; } = id;
        [Column] public string? PayloadStr { get; } = payloadStr;
        [Column] public double PayloadDouble { get; } = payloadDouble;
        [Column] public DateTime PayloadTimestamp { get; } = payloadTimestamp;
    }
}