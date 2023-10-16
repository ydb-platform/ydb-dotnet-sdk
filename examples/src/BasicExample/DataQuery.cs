using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Examples;

internal partial class BasicExample
{
    private async Task SimpleSelect(ulong id)
    {
        var response = await Client.SessionExec(async session =>
        {
            var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    DECLARE $id AS Uint64;

                    SELECT
                        series_id,
                        title,
                        release_date
                    FROM series
                    WHERE series_id = $id;
                ";

            return await session.ExecuteDataQuery(
                query: query,
                txControl: TxControl.BeginSerializableRW().Commit(),
                parameters: new Dictionary<string, YdbValue>
                {
                    { "$id", YdbValue.MakeUint64(id) }
                },
                settings: DefaultCachedDataQuerySettings
            );
        });

        response.Status.EnsureSuccess();

        var queryResponse = (ExecuteDataQueryResponse)response;
        var resultSet = queryResponse.Result.ResultSets[0];

        Console.WriteLine($"> SimpleSelect, " +
                          $"columns: {resultSet.Columns.Count}, " +
                          $"rows: {resultSet.Rows.Count}, " +
                          $"truncated: {resultSet.Truncated}");

        foreach (var row in resultSet.Rows)
        {
            Console.WriteLine($"> Series, " +
                              $"series_id: {(ulong?)row["series_id"]}, " +
                              $"title: {(string?)row["title"]}, " +
                              $"release_date: {(DateTime?)row["release_date"]}");
        }
    }

    private async Task SimpleUpsert(ulong id, string title, DateTime date)
    {
        var response = await Client.SessionExec(async session =>
        {
            var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    DECLARE $id AS Uint64;
                    DECLARE $title AS Utf8;
                    DECLARE $release_date AS Date;

                    UPSERT INTO series (series_id, title, release_date) VALUES
                        ($id, $title, $release_date);
                ";

            return await session.ExecuteDataQuery(
                query: query,
                txControl: TxControl.BeginSerializableRW().Commit(),
                parameters: new Dictionary<string, YdbValue>
                {
                    { "$id", YdbValue.MakeUint64(id) },
                    { "$title", YdbValue.MakeUtf8(title) },
                    { "$release_date", YdbValue.MakeDate(date) }
                },
                settings: DefaultCachedDataQuerySettings
            );
        });

        response.Status.EnsureSuccess();
    }
}