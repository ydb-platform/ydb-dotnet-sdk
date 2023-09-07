namespace slo.Jobs;

internal class WriteJob : Job
{
    public WriteJob(Table table, RateLimitedCaller rateLimitedCaller) : base(table, rateLimitedCaller, "write")
    {
    }


    protected override async Task PerformQuery()
    {
        var parameters = DataGenerator.GetUpsertData();

        await Table.Executor.ExecuteDataQuery(
            Queries.GetWriteQuery(Table.TableName),
            parameters,
            AttemptsHistogram
        );
    }
}