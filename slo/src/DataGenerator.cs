using Ydb.Sdk.Value;

namespace slo;

public static class DataGenerator
{
    private static readonly Random Random = new();

    public static int MaxId { get; private set; }

    public static async Task LoadMaxId(string tableName, Executor executor)
    {
        var response = await executor.ExecuteDataQuery(Queries.GetLoadMaxIdQuery(tableName));
        var row = response.Result.ResultSets[0].Rows[0];
        var value = row[0];
        MaxId = (int?)value.GetOptionalUint64() ?? 0;
    }

    public static Dictionary<string, YdbValue> GetUpsertData()
    {
        MaxId++;
        return new Dictionary<string, YdbValue>
        {
            { "$id", YdbValue.MakeUint64((ulong)MaxId) },
            {
                "$payload_str",
                YdbValue.MakeUtf8(string.Join("", Enumerable
                    .Repeat(0, Random.Next(20, 40))
                    .Select(_ => (char)new Random().Next(127))))
            },
            { "$payload_double", YdbValue.MakeDouble(Random.NextDouble()) },
            { "$payload_timestamp", YdbValue.MakeTimestamp(DateTime.Now) }
        };
    }
}