using Dapper;
using Internal;
using Polly;
using Ydb.Sdk;
using Ydb.Sdk.Ado;

namespace AdoNet.Dapper;

public class SloTableContext : SloTableContext<YdbDataSource>
{
    private static readonly AsyncPolicy Policy = Polly.Policy
        .Handle<YdbException>(exception => exception.IsTransient)
        .RetryAsync(10);

    protected override string Job => "Dapper";

    protected override YdbDataSource CreateClient(Config config) => new(
        new YdbConnectionStringBuilder(config.ConnectionString) { LoggerFactory = ISloContext.Factory }
    );

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

    protected override async Task<(int, StatusCode)> Save(YdbDataSource client, SloTable sloTable, int writeTimeout)
    {
        var attempt = 0;
        var policyResult = await Policy.ExecuteAndCaptureAsync(async _ =>
            {
                attempt++;
                await using var connection = await client.OpenConnectionAsync();
                await connection.ExecuteAsync($"""
                                               UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
                                               VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp)
                                               """, sloTable);
            }, new Context()
        );

        return (attempt, ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success);
    }

    protected override async Task<(int, StatusCode, object?)> Select(YdbDataSource client, (Guid Guid, int Id) select,
        int readTimeout)
    {
        var attempts = 0;
        var policyResult = await Policy.ExecuteAndCaptureAsync(async _ =>
        {
            attempts++;
            await using var connection = await client.OpenConnectionAsync();

            return await connection.QueryFirstOrDefaultAsync<SloTable>(
                $"""
                 SELECT Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp
                 FROM `{SloTable.Name}` WHERE Guid = @Guid AND Id = @Id;
                 """,
                new { select.Guid, select.Id }
            );
        }, new Context());

        return (attempts, ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success, policyResult.Result);
    }

    protected override async Task<int> SelectCount(YdbDataSource client)
    {
        await using var connection = await client.OpenConnectionAsync();
        return await connection.ExecuteScalarAsync<int>($"SELECT MAX(Id) FROM {SloTable.Name}");
    }
}