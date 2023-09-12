namespace slo.Jobs;

internal class WriteJob : Job
{
    public WriteJob(Table table, RateLimitedCaller rateLimitedCaller, TimeSpan timeout) : base(table, rateLimitedCaller,
        "write", timeout)
    {
    }


    protected override async Task PerformQuery()
    {
        var parameters = DataGenerator.GetUpsertData();

        await Table.Executor.ExecuteDataQuery(
            Queries.GetWriteQuery(Table.TableName),
            parameters,
            AttemptsHistogram,
            Timeout
        );
    }
}