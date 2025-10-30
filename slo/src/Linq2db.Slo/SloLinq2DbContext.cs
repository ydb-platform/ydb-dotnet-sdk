using System;
using System.Threading.Tasks;
using Internal;
using LinqToDB;
using LinqToDB.Data; // <= вот это нужно для DataConnection

namespace Linq2db;

public sealed class SloTableContext : SloTableContext<SloTableContext.Linq2dbClient>
{
    protected override string Job => "Linq2DB";

    public sealed class Linq2dbClient
    {
        private readonly string _connectionString;

        public Linq2dbClient(string connectionString)
        {
            _connectionString = connectionString;
        }

        public DataConnection Open() => new DataConnection("YDB", _connectionString);
    }

    protected override Linq2dbClient CreateClient(Config config)
        => new Linq2dbClient(config.ConnectionString);

    protected override async Task Create(Linq2dbClient client, int operationTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = operationTimeout;

        // 1) CREATE TABLE
        await db.ExecuteAsync($@"
            CREATE TABLE `{SloTable.Name}` (
                Guid             Uuid,
                Id               Int32,
                PayloadStr       Text,
                PayloadDouble    Double,
                PayloadTimestamp Timestamp,
                PRIMARY KEY (Guid, Id)
            );
        ");

        await db.ExecuteAsync(SloTable.Options);
    }

    protected override async Task<int> Save(Linq2dbClient client, SloTable row, int writeTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = writeTimeout;

        await db.ExecuteAsync($@"
            UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
            VALUES ({row.Guid}, {row.Id}, {row.PayloadStr}, {row.PayloadDouble}, {row.PayloadTimestamp});
        ");

        return 1;
    }

    protected override async Task<object?> Select(Linq2dbClient client, (Guid Guid, int Id) key, int readTimeout)
    {
        await using var db = client.Open();
        db.CommandTimeout = readTimeout;

        var exists = await db.ExecuteAsync<int>($@"
            SELECT COUNT(*) FROM `{SloTable.Name}` WHERE Guid = {key.Guid} AND Id = {key.Id};
        ");

        return exists > 0 ? 1 : null;
    }

    protected override async Task<int> SelectCount(Linq2dbClient client)
    {
        await using var db = client.Open();

        var maxId = await db.ExecuteAsync<int?>($@"
            SELECT MAX(Id) FROM `{SloTable.Name}`;
        ");

        return maxId ?? 0;
    }
}
