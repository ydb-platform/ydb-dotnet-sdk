using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Examples;

internal partial class BasicExample
{
    private async Task ScanQuery(DateTime airFrom)
    {
        var query = @$"
                PRAGMA TablePathPrefix('{BasePath}');

                DECLARE $air_from AS Date;

                SELECT series_id, season_id, COUNT(*) AS episodes_count
                FROM episodes
                WHERE air_date >= $air_from
                GROUP BY series_id, season_id
                ORDER BY series_id, season_id;
            ";

        var scanStream = Client.ExecuteScanQuery(
            query,
            new Dictionary<string, YdbValue>
            {
                { "$air_from", YdbValue.MakeDate(airFrom) }
            });

        while (await scanStream.Next())
        {
            scanStream.Response.EnsureSuccess();

            var resultSet = scanStream.Response.Result.ResultSetPart;
            if (resultSet != null)
            {
                foreach (var row in resultSet.Rows)
                {
                    Console.WriteLine($"> ScanQuery, " +
                                      $"series_id: {(ulong?)row["series_id"]}, " +
                                      $"season_id: {(ulong?)row["season_id"]}, " +
                                      $"episodes_count: {(ulong)row["episodes_count"]}");
                }
            }
        }
    }
}