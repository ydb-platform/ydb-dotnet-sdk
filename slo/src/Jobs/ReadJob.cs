using Ydb.Sdk.Value;

namespace slo.Jobs;

internal class ReadJob : Job
{
    public ReadJob(Table table, RateLimitedCaller rateLimitedCaller) : base(table, rateLimitedCaller, "read")
    {
    }


    protected override async Task PerformQuery()
    {
        var parameters = new Dictionary<string, YdbValue>
        {
            { "$id", YdbValue.MakeUint64((ulong)Random.Next(DataGenerator.MaxId)) }
        };

        await Table.Executor.ExecuteDataQuery(
            Queries.GetReadQuery(Table.TableName),
            parameters,
            AttemptsHistogram
        );
    }
}