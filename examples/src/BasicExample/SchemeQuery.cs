using System.Threading.Tasks;

namespace Ydb.Sdk.Examples;

internal partial class BasicExample
{
    // Execute Scheme (DDL) query to create sample tables.
    private async Task SchemeQuery()
    {
        var response = await Client.SessionExec(async session =>
            await session.ExecuteSchemeQuery(@$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    CREATE TABLE series (
                        series_id Uint64,
                        title Utf8,
                        series_info Utf8,
                        release_date Date,
                        PRIMARY KEY (series_id)
                    );

                    CREATE TABLE seasons (
                        series_id Uint64,
                        season_id Uint64,
                        title Utf8,
                        first_aired Date,
                        last_aired Date,
                        PRIMARY KEY (series_id, season_id)
                    );

                    CREATE TABLE episodes (
                        series_id Uint64,
                        season_id Uint64,
                        episode_id Uint64,
                        title Utf8,
                        air_date Date,
                        PRIMARY KEY (series_id, season_id, episode_id)
                    );
                "));

        response.Status.EnsureSuccess();
    }
}