using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Examples;

internal partial class BasicExample
{
    private async Task InteractiveTx()
    {
        var execResponse = await Client.SessionExec(async session =>
        {
            var query1 = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    DECLARE $series_id AS Uint64;
                    DECLARE $season_id AS Uint64;

                    SELECT first_aired FROM seasons
                    WHERE series_id = $series_id AND season_id = $season_id;
                ";

            // Execute first query (no transaction commit)
            var response = await session.ExecuteDataQuery(
                query: query1,
                txControl: TxControl.BeginSerializableRW(),
                parameters: new Dictionary<string, YdbValue>
                {
                    { "$series_id", YdbValue.MakeUint64(1) },
                    { "$season_id", YdbValue.MakeUint64(3) }
                },
                settings: DefaultCachedDataQuerySettings
            );

            if (!response.Status.IsSuccess || response.Tx is null)
            {
                return response;
            }

            // Perform some client logic
            var firstAired = (DateTime?)response.Result.ResultSets[0].Rows[0]["first_aired"];
            var newAired = firstAired!.Value.AddDays(2);

            var query2 = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    DECLARE $series_id AS Uint64;
                    DECLARE $season_id AS Uint64;
                    DECLARE $air_date AS Date;

                    UPSERT INTO seasons (series_id, season_id, first_aired) VALUES
                        ($series_id, $season_id, $air_date);
                ";

            // Execute second query and commit transaction.
            response = await session.ExecuteDataQuery(
                query: query2,
                TxControl.Tx(response.Tx).Commit(),
                parameters: new Dictionary<string, YdbValue>
                {
                    { "$series_id", YdbValue.MakeUint64(1) },
                    { "$season_id", YdbValue.MakeUint64(3) },
                    { "$air_date", YdbValue.MakeDate(newAired) }
                },
                settings: DefaultCachedDataQuerySettings
            );

            return response;
        });

        execResponse.Status.EnsureSuccess();
    }
}