using System.Security.Cryptography.X509Certificates;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Services.Query;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Examples;

public class QueryExample
{
    private QueryClient Client { get; }
    private string BasePath { get; }

    private QueryExample(QueryClient client, string database, string path)
    {
        Client = client;
        BasePath = string.Join('/', database, path);
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

        await using var tableClient = new QueryClient(driver, new QueryClientConfig());

        var example = new QueryExample(tableClient, database, path);

        await example.SchemeQuery();
        await example.FillData();
        await example.SimpleSelect(1);
        await example.SimpleUpsert(10, "Coming soon", DateTime.UtcNow);
        await example.SimpleSelect(10);
        await example.InteractiveTx();
        await example.StreamSelect();
        await example.ReadScalar();
        await example.ReadSingleRow();
        await example.ReadAllRows();
        await example.ReadAllResultSets();
    }

    private static ExecuteQuerySettings DefaultQuerySettings =>
        new()
        {
            // Transport timeout from the moment operation was sent to server. It is useful in case
            // of possible network issues, to that query doesn't hang forever.
            // It is recommended to set this value to a larger value than OperationTimeout to give
            // server some time to issue a response.
            TransportTimeout = TimeSpan.FromSeconds(5),

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


        await Client.Exec(
            query: createQuery
        );
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

        await Client.Exec(
            query: query,
            parameters: DataUtils.GetDataParams(),
            txMode: TxMode.SerializableRw,
            settings: DefaultQuerySettings
        );
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


        var response = await Client.ReadAllRows(
            query,
            parameters: parameters
        );

        foreach (var row in response)
        {
            var series = Series.FromRow(row);
            Console.WriteLine("> Series, " +
                              $"series_id: {series.SeriesId}, " +
                              $"title: {series.Title}, " +
                              $"release_date: {series.ReleaseDate}");
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

        await Client.Exec(
            query,
            parameters
        );
    }


    private Task InteractiveTx() =>
        Client.DoTx(async tx =>
            {
                var query1 = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT first_aired FROM seasons
                    WHERE series_id = $series_id AND season_id = $season_id
                    LIMIT 1;
                ";
                var parameters1 = new Dictionary<string, YdbValue>
                {
                    { "$series_id", YdbValue.MakeUint64(1) },
                    { "$season_id", YdbValue.MakeUint64(3) }
                };

                var response = await tx.ReadRow(query1, parameters1);

                var newAired = response![0].GetOptionalDate()!.Value.AddDays(2);

                var query2 = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    UPSERT INTO seasons (series_id, season_id, first_aired) VALUES
                        ($series_id, $season_id, $first_aired);
                ";
                var parameters2 = new Dictionary<string, YdbValue>
                {
                    { "$series_id", YdbValue.MakeUint64(1) },
                    { "$season_id", YdbValue.MakeUint64(3) },
                    { "$first_aired", YdbValue.MakeDate(newAired) }
                };

                await tx.Exec(query2, parameters2);
            }
        );

    private async Task StreamSelect()
    {
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT *
                    FROM series;
                ";


        await Client.DoTx(async tx =>
        {
            await foreach (var part in await tx.Stream(query, commit: true))
            {
                foreach (var row in part.ResultSet!.Rows)
                {
                    Console.WriteLine(Series.FromRow(row));
                }
            }
        });
    }

    private async Task ReadScalar()
    {
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT COUNT(*)
                    FROM series;
                ";


        var row = await Client.ReadRow(query);

        var count = row![0].GetUint64();

        Console.WriteLine($"There is {count} rows in 'series' table");
    }

    private async Task ReadSingleRow()
    {
        Console.WriteLine("StreamSelect");
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT *
                    FROM series
                    LIMIT 1;
                ";


        var row = await Client.ReadRow(query);

        var series = Series.FromRow(row!);

        Console.WriteLine($"First row in 'series' table is {series}");
    }

    private async Task ReadAllRows()
    {
        Console.WriteLine("StreamSelect");
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT *
                    FROM series;
                ";


        var response = await Client.ReadAllRows(query);

        var series = response.Select(Series.FromRow);

        Console.WriteLine("'series' table contains:");
        foreach (var elem in series)
        {
            Console.WriteLine($"\t{elem}");
        }
    }

    private async Task ReadAllResultSets()
    {
        Console.WriteLine("StreamSelect");
        var query = @$"
                    PRAGMA TablePathPrefix('{BasePath}');

                    SELECT *
                    FROM series; -- First result set

                    SELECT *
                    FROM episodes; -- Second result set
                ";


        var resultSets = await Client.DoTx(async tx =>
        {
            var resultSets = new List<Value.ResultSet>();
            await foreach (var resultSet in await tx.Stream(query, commit: true))
            {
                resultSets.Add(resultSet.ResultSet!);
            }

            return resultSets;
        });

        var seriesSet = resultSets[0];
        var episodesSet = resultSets[1];

        Console.WriteLine("Multiple sets selected:");

        Console.WriteLine("\t'series' contains:");
        foreach (var row in seriesSet.Rows)
        {
            Console.WriteLine($"\t\t{Series.FromRow(row)}");
        }

        Console.WriteLine("\t'episodes' contains:");
        foreach (var row in episodesSet.Rows)
        {
            Console.WriteLine($"\t\t{Episode.FromRow(row)}");
        }
    }
}