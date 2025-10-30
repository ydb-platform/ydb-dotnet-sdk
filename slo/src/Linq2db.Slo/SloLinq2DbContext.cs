using System;
using System.Threading.Tasks;
using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using Internal;
using LinqToDB.Async;
using LinqToDB.Internal.DataProvider.Ydb.Internal;

namespace Linq2db;

public sealed class SloTableContext : SloTableContext<SloTableContext.Linq2dbClient>
{
    protected override string Job => "Linq2DB";

    // ВКЛЮЧАЕМ ретраи SDK глобально для всех DataConnection
    static SloTableContext()
    {
        YdbSdkRetryPolicyRegistration.UseGloballyWithIdempotence(
            maxAttempts: 10,
            onRetry: (attempt, ex, delay) =>
            {
                // метрики/логи при желании
            }
        );
    }

    public sealed class Linq2dbClient
    {
        private readonly string _connectionString;
        public Linq2dbClient(string connectionString) => _connectionString = connectionString;
        public DataConnection Open() => new DataConnection("YDB", _connectionString);
    }

    protected override Linq2dbClient CreateClient(Config config)
        => new Linq2dbClient(config.ConnectionString);

    protected override async Task Create(Linq2dbClient client, int operationTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = operationTimeout;

        try
        {
            await db.ExecuteAsync($@"
CREATE TABLE `{SloTable.Name}` (
    Guid             Uuid,
    Id               Int32,
    PayloadStr       Utf8,
    PayloadDouble    Double,
    PayloadTimestamp Timestamp,
    PRIMARY KEY (Guid, Id)
)");
        }
        catch
        {
            // YDB не поддерживает IF NOT EXISTS; если таблица есть — это норм
        }

        if (!string.IsNullOrWhiteSpace(SloTable.Options))
            await db.ExecuteAsync(SloTable.Options);
    }

    // ВАЖНО: вернуть >0 при успехе, иначе write-графики будут пустые.
    protected override async Task<int> Save(Linq2dbClient client, SloTable sloTable, int writeTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = writeTimeout;

        await db.ExecuteAsync($@"
UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
VALUES ({sloTable.Guid}, {sloTable.Id}, {sloTable.PayloadStr}, {sloTable.PayloadDouble}, {sloTable.PayloadTimestamp});
");

        return 1;
    }

    protected override async Task<object?> Select(Linq2dbClient client, (Guid Guid, int Id) select, int readTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = readTimeout;

        var t = db.GetTable<SloRow>();
        return await t.FirstOrDefaultAsync(r => r.Guid == select.Guid && r.Id == select.Id);
    }

    protected override async Task<int> SelectCount(Linq2dbClient client)
    {
        await using var db = client.Open();
        return await db.GetTable<SloRow>().CountAsync();
    }

    [Table(SloTable.Name)]
    private sealed class SloRow
    {
        [Column] public Guid Guid { get; set; }
        [Column] public int Id { get; set; }
        [Column] public string?  PayloadStr { get; set; }
        [Column] public double   PayloadDouble { get; set; }
        [Column] public DateTime PayloadTimestamp { get; set; }
    }
}
