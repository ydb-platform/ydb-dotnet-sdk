using System.Data;
using Internal;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace AdoNet;

public class SloTableContext : SloTableContext<YdbDataSource>
{
    protected override string Job => "AdoNet";

    protected override YdbDataSource CreateClient(Config config) => new YdbDataSourceBuilder(
        new YdbConnectionStringBuilder(config.ConnectionString) { LoggerFactory = ISloContext.Factory }
    ) { RetryPolicy = new YdbRetryPolicy(new YdbRetryPolicyConfig { EnableRetryIdempotence = true }) }.Build();

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

    protected override async Task<int> Save(
        YdbDataSource client,
        SloTable sloTable,
        int writeTimeout
    )
    {
        var attempts = 0;
        await client.ExecuteAsync(async ydbConnection =>
        {
            attempts++;
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
        });


        return attempts;
    }

    protected override async Task<object?> Select(
        YdbDataSource client,
        (Guid Guid, int Id) select,
        int readTimeout
    )
    {
        await using var ydbConnection = await client.OpenRetryableConnectionAsync();

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
    }

    protected override async Task<int> SelectCount(YdbDataSource client)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();

        return (int)(await new YdbCommand(ydbConnection) { CommandText = $"SELECT MAX(Id) FROM {SloTable.Name}" }
            .ExecuteScalarAsync())!;
    }
}