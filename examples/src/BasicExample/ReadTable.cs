using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Examples;

internal partial class BasicExample
{
    private async Task ReadTable()
    {
        var readStream = Client.ReadTable(
            FullTablePath("seasons"),
            new ReadTableSettings
            {
                Columns = new List<string> { "series_id", "season_id", "first_aired" },
                RowLimit = 5,
                Ordered = true
            });

        while (await readStream.Next())
        {
            readStream.Response.EnsureSuccess();
            var resultSet = readStream.Response.Result.ResultSet;

            foreach (var row in resultSet.Rows)
            {
                Console.WriteLine($"> ReadTable seasons, " +
                                  $"series_id: {(ulong?)row["series_id"]}, " +
                                  $"season_id: {(ulong?)row["season_id"]}, " +
                                  $"first_aired: {(DateTime?)row["first_aired"]}");
            }
        }
    }
}