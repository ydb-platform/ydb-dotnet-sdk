using Ydb.Sdk.Value;

namespace slo;

public class DataGenerator
{
    private static Random _random = new();

    public static int MaxId { get; set; }


    public static int GetRandomId() => _random.Next(MaxId);

    public static async Task LoadMaxId(string tableName, Executor executor)
    {
        var response = await executor.ExecuteDataQuery(Queries.GetLoadMaxIdQuery(tableName));
        var row = response.Result.ResultSets[0].Rows[0];
        var value = row[0];
        MaxId = (int)(value.GetOptionalUint64() ?? 0);
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
                    .Repeat(0, _random.Next(20, 40))
                    .Select(_ => (char)new Random().Next(127))))
            },
            { "$payload_double", YdbValue.MakeDouble(_random.NextDouble()) },
            { "$payload_timestamp", YdbValue.MakeTimestamp(DateTime.Now) },
        };
    }
}