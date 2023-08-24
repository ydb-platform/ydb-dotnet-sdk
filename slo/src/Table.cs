namespace slo;

public class Table : IAsyncDisposable
{
    public readonly string TableName;
    public readonly Executor Executor;

    private Table(string tableName, Executor executor)
    {
        TableName = tableName;
        Executor = executor;
    }

    private static void GenerateContent()
    {
    }

    public static async Task<Table> Create(string tableName, Executor executor)
    {
        await executor.ExecuteSchemeQuery(Queries.GetCreateQuery(tableName));

        await DataGenerator.LoadMaxId(tableName, executor);
        
        var tasks = new List<Task> { Capacity = 20 };

        for (var i = 0; i < 20; i++)
            tasks.Add(executor.ExecuteDataQuery(Queries.GetWriteQuery(tableName), DataGenerator.GetUpsertData()));

        await Task.WhenAll(tasks);


        return new Table(tableName, executor);
    }


    public async ValueTask DisposeAsync()
    {
        Console.WriteLine("DISPOSE");
        await Executor.ExecuteSchemeQuery(Queries.GetDropQuery(TableName));
    }
}