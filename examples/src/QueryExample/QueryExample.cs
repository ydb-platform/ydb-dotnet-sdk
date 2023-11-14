using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Services.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Examples;

public class QueryExample
{
    private QueryClient Client { get; }
    private string BasePath { get; }

    private Driver Driver { get; }

    protected QueryExample(QueryClient client, string database, string path, Driver driver)
    {
        Client = client;
        BasePath = string.Join('/', database, path);
        Driver = driver;
    }


    public static async Task Run(
        string endpoint,
        string database,
        ICredentialsProvider credentialsProvider,
        X509Certificate? customServerCertificate,
        string path,
        ILoggerFactory loggerFactory)
    {
        var config = new DriverConfig(
            endpoint: endpoint,
            database: database,
            credentials: credentialsProvider,
            customServerCertificate: customServerCertificate
        );

        await using var driver = await Driver.CreateInitialized(
            config: config,
            loggerFactory: loggerFactory
        );

        using var tableClient = new QueryClient(driver, new QueryClientConfig());

        var example = new QueryExample(tableClient, database, path, driver);

        await example.SchemeQuery();
        await example.FillData();
        await example.SimpleSelect(1);
        await example.SimpleUpsert(10, "Coming soon", DateTime.UtcNow);
        await example.SimpleSelect(10);
        await example.InteractiveTx();
    }

    private static ExecuteQuerySettings DefaultQuerySettings =>
        new()
        {
            // Transport timeout from the moment operation was sent to server. It is useful in case
            // of possible network issues, to that query doesn't hang forever.
            // It is recommended to set this value to a larger value than OperationTimeout to give
            // server some time to issue a response.
            TransportTimeout = TimeSpan.FromSeconds(5),

            ExecMode = ExecMode.Execute,
            Syntax = Syntax.YqlV1
        };


    private async Task SchemeQuery()
    {
        var createQuery = @$"
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
                ";

        // TODO replace with QueryClient
        // var response = await Client.Query(
        //     queryString: createQuery,
        //     func: EmptyStreamFunc
        // );
        // response.EnsureSuccess();
        using var client = new TableClient(Driver);
        var response = await client.SessionExec(async session =>
            await session.ExecuteSchemeQuery(createQuery));

        response.Status.EnsureSuccess();
    }

    private async Task FillData()
    {
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    REPLACE INTO series
                    SELECT * FROM AS_TABLE($seriesData);

                    REPLACE INTO seasons
                    SELECT * FROM AS_TABLE($seasonsData);

                    REPLACE INTO episodes
                    SELECT * FROM AS_TABLE($episodesData);
                ";

        var response = await Client.NonQuery(
            queryString: query,
            parameters: DataUtils.GetDataParams(),
            txModeSettings: new TxModeSerializableSettings(),
            executeQuerySettings: DefaultQuerySettings
        );
        response.EnsureSuccess();
    }


    private async Task SimpleSelect(ulong id)
    {
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT *
                    FROM series
                    WHERE series_id = $id;
                ";

        var parameters = new Dictionary<string, YdbValue> { { "$id", (YdbValue)id } };


        var response = await Client.Query(
            query,
            parameters: parameters,
            async stream =>
            {
                var series = new List<Series>();
                await foreach (var part in stream)
                {
                    var resultSet = part.ResultSet;
                    if (resultSet is not null)
                    {
                        series.AddRange(resultSet.Rows.Select(Series.FromRow));
                    }
                }

                return series;
            }
        );

        response.EnsureSuccess();

        if (response.Result is not null)
        {
            foreach (var series in response.Result)
            {
                Console.WriteLine("> Series, " +
                                  $"series_id: {series.SeriesId}, " +
                                  $"title: {series.Title}, " +
                                  $"release_date: {series.ReleaseDate}");
            }
        }
    }


    private async Task SimpleUpsert(ulong id, string title, DateTime date)
    {
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    UPSERT INTO series (series_id, title, release_date) VALUES
                        ($id, $title, $release_date);
                ";
        var parameters = new Dictionary<string, YdbValue>
        {
            { "$id", YdbValue.MakeUint64(id) },
            { "$title", YdbValue.MakeUtf8(title) },
            { "$release_date", YdbValue.MakeDate(date) }
        };

        var response = await Client.NonQuery(
            query,
            parameters
        );
        response.EnsureSuccess();
    }


    private async Task InteractiveTx()
    {
        var doTxResponse = await Client.DoTx(
            func: async tx =>
            {
                var query1 = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT first_aired FROM seasons
                    WHERE series_id = $series_id AND season_id = $season_id;
                ";
                var parameters1 = new Dictionary<string, YdbValue>
                {
                    { "$series_id", YdbValue.MakeUint64(1) },
                    { "$season_id", YdbValue.MakeUint64(3) }
                };

                var response = await tx.Query(
                    query1,
                    parameters1,
                    func: async stream =>
                    {
                        var result = new List<DateTime>();
                        await foreach (var part in stream)
                        {
                            var resultSet = part.ResultSet;
                            if (resultSet is not null)
                            {
                                result.AddRange(resultSet.Rows.Select(row =>
                                    (DateTime)row["first_aired"].GetOptionalDate()!));
                            }
                        }

                        return result;
                    }
                );
                response.EnsureSuccess();

                var firstAired = response.Result!.FirstOrDefault();
                var newAired = firstAired!.AddDays(2);

                var query2 = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    UPSERT INTO seasons (series_id, season_id, first_aired) VALUES
                        ($series_id, $season_id, $air_date);
                ";
                var parameters2 = new Dictionary<string, YdbValue>
                {
                    { "$series_id", YdbValue.MakeUint64(1) },
                    { "$season_id", YdbValue.MakeUint64(3) },
                    { "$air_date", YdbValue.MakeDate(newAired) }
                };

                var response2 = await tx.NonQuery(query2, parameters2);
                response2.EnsureSuccess();
            }
        );
        doTxResponse.EnsureSuccess();
    }
}