using Ydb.Sdk.Value;

namespace slo.Jobs;

internal class ReadJob : Job
{
    public ReadJob(Client client, RateLimitedCaller rateLimitedCaller, TimeSpan timeout) : base(client, rateLimitedCaller, "read", timeout)
    {
    }


    protected override async Task PerformQuery()
    {
        var parameters = new Dictionary<string, YdbValue>
        {
            { "$id", YdbValue.MakeUint64((ulong)Random.Next(DataGenerator.MaxId)) }
        };

        await Client.Executor.ExecuteDataQuery(
            Queries.GetReadQuery(Client.TableName),
            parameters,
            AttemptsHistogram,
            Timeout
        );
    }
}