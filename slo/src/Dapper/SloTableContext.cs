using Dapper;
using Internal;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace AdoNet.Dapper;

public class SloTableContext : SloTableContext<YdbDataSource>
{
    protected override string Job => "Dapper";

    protected override YdbDataSource CreateClient(Config config) => new YdbDataSourceBuilder(
        new YdbConnectionStringBuilder(config.ConnectionString) { LoggerFactory = ISloContext.Factory }
    ) { RetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig { EnableRetryIdempotence = true }) }.Build();

    protected override async Task Create(YdbDataSource client, int operationTimeout)
    {
        await using var connection = await client.OpenConnectionAsync();
        await connection.ExecuteAsync($"""
                                       CREATE TABLE `{SloTable.Name}` (
                                           Guid             Uuid,
                                           Id               Int32,
                                           PayloadStr       Text,
                                           PayloadDouble    Double,
                                           PayloadTimestamp Timestamp,
                                           PRIMARY KEY (Guid, Id)
                                       ); 
                                       {SloTable.Options}
                                       """);
    }

    protected override async Task<int> Save(YdbDataSource client, SloTable sloTable, int writeTimeout)
    {
        await using var ydbConnection =
            await client.OpenRetryableConnectionAsync(new YdbRetryPolicyConfig { EnableRetryIdempotence = true });
        await ydbConnection.ExecuteAsync(
            $"""
             UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
             VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp)
             """, sloTable);

        return 1;
    }

    protected override async Task<object?> Select(YdbDataSource client, (Guid Guid, int Id) select,
        int readTimeout)
    {
        await using var ydbConnection =
            await client.OpenRetryableConnectionAsync(new YdbRetryPolicyConfig { EnableRetryIdempotence = true });
        return await ydbConnection.QueryFirstOrDefaultAsync<SloTable>(
            $"""
             SELECT Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp
             FROM `{SloTable.Name}` WHERE Guid = @Guid AND Id = @Id;
             """, new { select.Guid, select.Id }
        );
    }

    protected override async Task<int> SelectCount(YdbDataSource client)
    {
        await using var connection = await client.OpenConnectionAsync();
        return await connection.ExecuteScalarAsync<int>($"SELECT MAX(Id) FROM {SloTable.Name}");
    }
}