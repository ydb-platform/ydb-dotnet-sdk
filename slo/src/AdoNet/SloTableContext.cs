using System.Data;
using Internal;
using Microsoft.Extensions.Logging;
using Polly;
using Prometheus;
using Ydb.Sdk;
using Ydb.Sdk.Ado;

namespace AdoNet;

public class SloTableContext : SloTableContext<YdbDataSource>
{
    private readonly AsyncPolicy _policy = Policy.Handle<YdbException>(exception => exception.IsTransient)
        .WaitAndRetryAsync(10, attempt => TimeSpan.FromMilliseconds(attempt * 10),
            (e, _, _, context) =>
            {
                var errorsTotal = (Counter)context["errorsTotal"];

                Logger.LogWarning(e, "Failed read / write operation");
                errorsTotal?.WithLabels(((YdbException)e).Code.StatusName(), "retried").Inc();
            });

    protected override string Job => "AdoNet";

    protected override async Task Create(YdbDataSource client, int operationTimeout)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();
        await new YdbCommand(ydbConnection)
        {
            CommandText = $"""
                           CREATE TABLE `{SloTable.Name}` (
                               Guid             UUID,
                               Id               Int32,
                               PayloadStr       Text,
                               PayloadDouble    Double,
                               PayloadTimestamp Timestamp,
                               PRIMARY KEY (hash, id)
                           )
                           """,
            CommandTimeout = operationTimeout
        }.ExecuteNonQueryAsync();
    }

    protected override async Task<(int, StatusCode)> Save(
        YdbDataSource dataSource,
        SloTable sloTable,
        int writeTimeout,
        Counter? errorsTotal = null
    )
    {
        var context = new Context();
        if (errorsTotal != null)
        {
            context["errorsTotal"] = errorsTotal;
        }

        var policyResult = await _policy.ExecuteAndCaptureAsync(async _ =>
        {
            await using var ydbConnection = await dataSource.OpenConnectionAsync();

            var ydbCommand = new YdbCommand(ydbConnection)
            {
                CommandText = $"""
                               INSERT INTO `{SloTable.Name}` (Guid, Id, PayloadStr, PayloadDouble, PayloadTimestamp)
                               VALUES (@Guid, @Id, @PayloadStr, @PayloadDouble, @PayloadTimestamp)
                               """,
                CommandTimeout = writeTimeout,
                Parameters =
                {
                    new YdbParameter
                        { DbType = DbType.Guid, ParameterName = "Guid", Value = sloTable.Guid },
                    new YdbParameter
                        { DbType = DbType.Int32, ParameterName = "Id", Value = sloTable.Id },
                    new YdbParameter
                        { DbType = DbType.String, ParameterName = "PayloadStr", Value = sloTable.PayloadStr },
                    new YdbParameter
                        { DbType = DbType.Double, ParameterName = "PayloadDouble", Value = sloTable.PayloadDouble },
                    new YdbParameter
                        { DbType = DbType.Guid, ParameterName = "PayloadTimestamp", Value = sloTable.PayloadTimestamp }
                }
            };

            await ydbCommand.ExecuteNonQueryAsync();
        }, context);


        return (policyResult.Context.TryGetValue("RetryCount", out var countAttempts) ? (int)countAttempts : 1,
            ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success);
    }

    protected override async Task<(int, StatusCode, object?)> Select(
        YdbDataSource dataSource,
        dynamic select,
        int readTimeout,
        Counter? errorsTotal = null
    )
    {
        var context = new Context();
        if (errorsTotal != null)
        {
            context["errorsTotal"] = errorsTotal;
        }

        var attempts = 0;
        var policyResult = await _policy.ExecuteAndCaptureAsync(async _ =>
        {
            attempts++;
            await using var ydbConnection = await dataSource.OpenConnectionAsync();

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
        }, context);

        return (attempts, ((YdbException)policyResult.FinalException)?.Code ?? StatusCode.Success, policyResult.Result);
    }

    protected override async Task<int> SelectCount(YdbDataSource client, string sql)
    {
        await using var ydbConnection = await client.OpenConnectionAsync();

        return (int)(await new YdbCommand(ydbConnection) { CommandText = sql }.ExecuteScalarAsync())!;
    }

    protected override YdbDataSource CreateClient(string connectionString) =>
        new(new YdbConnectionStringBuilder(connectionString) { LoggerFactory = ISloContext.Factory });
}