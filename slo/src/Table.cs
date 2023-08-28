namespace slo;

public class Table
{
    public readonly Executor Executor;
    public readonly string TableName;

    public Table(string tableName, Executor executor)
    {
        TableName = tableName;
        Executor = executor;
    }

    public async Task Init(int initialDataCount, int partitionSize, int minPartitionsCount, int maxPartitionsCount)
    {
        await Executor.ExecuteSchemeQuery(Queries.GetCreateQuery(TableName, partitionSize, minPartitionsCount,
            maxPartitionsCount));

        await DataGenerator.LoadMaxId(TableName, Executor);

        var tasks = new List<Task> { Capacity = initialDataCount };

        for (var i = 0; i < initialDataCount; i++)
            tasks.Add(Executor.ExecuteDataQuery(Queries.GetWriteQuery(TableName), DataGenerator.GetUpsertData()));

        await Task.WhenAll(tasks);
    }

    public async Task CleanUp()
    {
        await Executor.ExecuteSchemeQuery(Queries.GetDropQuery(TableName));
    }
}