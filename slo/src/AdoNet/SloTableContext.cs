using System.Data;
using Internal;
using Microsoft.Extensions.Logging;
using Polly;
using Ydb.Sdk;
using Ydb.Sdk.Ado;

namespace AdoNet;

public class SloTableContext : SloTableContext<YdbDataSource>
{
    private static readonly AsyncPolicy Policy = Polly.Policy.Handle<YdbException>(exception => exception.IsTransient)
        .WaitAndRetryAsync(10, attempt => TimeSpan.FromMilliseconds(attempt * 10),
            (e, _, _, _) => { Logger.LogWarning(e, "Failed read / write operation"); });

    protected override string Job => "AdoNet";

    protected override YdbDataSource CreateClient(Config config) => new(
        new YdbConnectionStringBuilder(config.ConnectionString) { LoggerFactory = ISloContext.Factory }
    );

    protected override async Task Create(YdbDataSource client, int operationTimeout)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();
        await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           CREATE TABLE `{SloTable.Name}` (
                               Guid             Uuid,
                               Id               Int32,
                               PayloadStr       Text,
                               PayloadDouble    Double,
                               PayloadTimestamp Timestamp,
                               PRIMARY KEY (Guid, Id)
                           );
                           {SloTable.Options}
                           """,
            CommandTimeout = operationTimeout
        }.ExecuteNonQueryAsync();
    }

    protected override async Task<(int, StatusCode)> Save(
        YdbDataSource client,
        SloTable sloTable,
        int writeTimeout
    )
    {
        var attempts = 0;
        var policyResult = await Policy.ExecuteAndCaptureAsync(async _ =>
        {
            attempts++;
            await using var ydbConnection = await client.OpenConnectionAsync();

            var ydbCommand = new YdbCommand(ydbConnection)
            {
                CommandText = $"""
                               UPSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
                               VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp)
                               """,
                CommandTimeout = writeTimeout,
                Parameters =
                {
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
                    }
                }
            };

            await ydbCommand.ExecuteNonQueryAsync();
        }, new Context());


        return (attempts, ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success);
    }

    protected override async Task<(int, StatusCode, object?)> Select(
        YdbDataSource client,
        (Guid Guid, int Id) select,
        int readTimeout
    )
    {
        var attempts = 0;
        var policyResult = await Policy.ExecuteAndCaptureAsync(async _ =>
        {
            attempts++;
            await using var ydbConnection = await client.OpenConnectionAsync();

            var ydbCommand = new YdbCommand(ydbConnection)
            {
                CommandText = $"""
                               SELECT Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp
                               FROM `{SloTable.Name}` WHERE Guid = @Guid AND Id = @Id;
                               """,
                CommandTimeout = readTimeout,
                Parameters =
                {
                    new YdbParameter { ParameterName = "Guid", DbType = DbType.Guid, Value = select.Guid },
                    new YdbParameter { ParameterName = "Id", DbType = DbType.Int32, Value = select.Id }
                }
            };

            return await ydbCommand.ExecuteScalarAsync();
        }, new Context());

        return (attempts, ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success, policyResult.Result);
    }

    protected override async Task<int> SelectCount(YdbDataSource client)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();

        return (int)(await new YdbCommand(ydbConnection) { CommandText = $"SELECT MAX(Id) FROM {SloTable.Name}" }
            .ExecuteScalarAsync())!;
    }
}