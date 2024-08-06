namespace Internal.Jobs;

public class WriteJob : Job
{
    public WriteJob(Client client, RateLimitedCaller rateLimitedCaller, TimeSpan timeout) :
        base(client, rateLimitedCaller, "write", timeout)
    {
    }


    protected override async Task PerformQuery()
    {
        var parameters = DataGenerator.GetUpsertData();

        await Client.Executor.ExecuteDataQuery(
            Queries.GetWriteQuery(Client.TableName),
            parameters,
            Timeout,
            AttemptsHistogram,
            ErrorsGauge
        );
    }
}