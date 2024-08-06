using Internal.Cli;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Value;

namespace Internal;

public abstract class SloContext(ILogger logger)
{
    private volatile int _maxId;

    public async Task Create(CreateConfig config)
    {
        const int maxCreateAttempts = 10;

        for (var i = 0; i < maxCreateAttempts; i++)
        {
            try
            {
                await Create($"""
                              CREATE TABLE {config.TableName} (
                                  hash              Uint64,
                                  id                Uint64,
                                  payload_str       Text,
                                  payload_double    Double,
                                  payload_timestamp Timestamp,
                                  payload_hash      Uint64,
                                  PRIMARY KEY (hash, id)
                              ) WITH (
                                  AUTO_PARTITIONING_BY_SIZE = ENABLED,
                                  AUTO_PARTITIONING_BY_LOAD = ENABLED,
                                  AUTO_PARTITIONING_PARTITION_SIZE_MB = ${config.PartitionSize},
                                  AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = ${config.MinPartitionsCount},
                                  AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = ${config.MaxPartitionsCount},
                              );
                              """, config.WriteTimeout);
            }
            catch (Exception e)
            {
                logger.LogError(e, "Fail created table");

                if (i == maxCreateAttempts - 1)
                {
                    throw;
                }
            }
        }

        var tasks = new Task[config.InitialDataCount];
        for (var i = 0; i < config.InitialDataCount; i++)
        {
            tasks[i] = Upsert(config);
        }

        await Task.WhenAll(tasks);
    }

    protected abstract Task Create(string createTableSql, int operationTimeout);

    // public async Task Run(RunConfig runConfig)
    // {
    // }

    protected abstract Task Upsert(string upsertSql, Dictionary<string, YdbValue> parameters, int writeTimeout);

    protected abstract Task<string> Select(string selectSql, Dictionary<string, YdbValue> parameters, int readTimeout);

    // public async Task CleanUp(CleanUpConfig config)
    // {
    //     await CleanUp($"DROP TABLE ${config.TableName}", config.WriteTimeout);
    // }

    protected abstract Task CleanUp(string dropTableSql, int operationTimeout);

    private Task Upsert(Config config)
    {
        const int minSizeStr = 20;
        const int maxSizeStr = 40;

        return Upsert($"""
                       UPSERT INTO `{config.TableName}` (id, hash, payload_str, payload_double, payload_timestamp)
                       VALUES ($id, Digest::NumericHash($id), $payload_str, $payload_double, $payload_timestamp)
                       """, new Dictionary<string, YdbValue>
        {
            { "$id", YdbValue.MakeUint64((ulong)Interlocked.Increment(ref _maxId)) },
            {
                "$payload_str", YdbValue.MakeUtf8(string.Join(string.Empty, Enumerable
                    .Repeat(0, Random.Shared.Next(minSizeStr, maxSizeStr))
                    .Select(_ => (char)Random.Shared.Next(127))))
            },
            { "$payload_double", YdbValue.MakeDouble(Random.Shared.NextDouble()) },
            { "$payload_timestamp", YdbValue.MakeTimestamp(DateTime.Now) }
        }, config.WriteTimeout);
    }

//     private Task<string> Select(RunConfig config)
//     {
//         return Select(
//             $"""
//              SELECT id, payload_str, payload_double, payload_timestamp, payload_hash
//              FROM `{config.TableName}` WHERE id = $id AND hash = Digest::NumericHash($id)
//              """,
//             new Dictionary<string, YdbValue>
//             {
//                 { "$id", YdbValue.MakeUint64((ulong)Random.Shared.Next(_maxId)) }
//             }, config.ReadTimeout);
//     }
}