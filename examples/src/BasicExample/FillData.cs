using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Examples;

internal partial class BasicExample
{
    // Fill sample tables with initial data.
    private async Task FillData()
    {
        var response = await Client.SessionExec(async session =>
        {
            var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    DECLARE $seriesData AS List<Struct<
                        series_id: Uint64,
                        title: Utf8,
                        series_info: Utf8,
                        release_date: Date>>;

                    DECLARE $seasonsData AS List<Struct<
                        series_id: Uint64,
                        season_id: Uint64,
                        title: Utf8,
                        first_aired: Date,
                        last_aired: Date>>;

                    DECLARE $episodesData AS List<Struct<
                        series_id: Uint64,
                        season_id: Uint64,
                        episode_id: Uint64,
                        title: Utf8,
                        air_date: Date>>;

                    REPLACE INTO series
                    SELECT * FROM AS_TABLE($seriesData);

                    REPLACE INTO seasons
                    SELECT * FROM AS_TABLE($seasonsData);

                    REPLACE INTO episodes
                    SELECT * FROM AS_TABLE($episodesData);
                ";

            return await session.ExecuteDataQuery(
                query: query,
                txControl: TxControl.BeginSerializableRW().Commit(),
                parameters: DataUtils.GetDataParams(),
                settings: DefaultDataQuerySettings
            );
        });

        response.Status.EnsureSuccess();
    }
}