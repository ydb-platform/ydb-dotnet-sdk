using CommandLine;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Ydb.Sdk.Auth;
using Ydb.Sdk.Scheme;
using Ydb.Sdk.Table;
using Ydb.Sdk.Value;

namespace Ydb.Sdk.Examples
{
    class CmdOptions
    {
        [Option('e', "endpoint", Required = true, HelpText = "Database endpoint")]
        public string Endpoint { get; set; } = String.Empty;

        [Option('d', "database", Required = true, HelpText = "Database name")]
        public string Database { get; set; } = String.Empty;
    }
    class BasicExample
    {
        private static ServiceProvider GetServiceProvider()
        {
            return new ServiceCollection()
               .AddLogging(configure => configure.AddConsole().SetMinimumLevel(LogLevel.Trace))
               .BuildServiceProvider();
        }

        static async Task Run(CmdOptions options)
        {
            using var serviceProvider = GetServiceProvider();
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();

            ICredentialsProvider? credentials = null;
            var token = Environment.GetEnvironmentVariable("YDB_ACCESS_TOKEN_CREDENTIALS");
            if (token != null)
            {
                credentials = new TokenProvider(token);
            }

            var config = new DriverConfig(
                endpoint: options.Endpoint,
                database: options.Database,
                credentials: credentials
            );

            using var driver = new Driver(
                config: config,
                loggerFactory: loggerFactory
            );
            await driver.Initialize();

            SchemeClient schemeClient = new SchemeClient(driver);
            ListDirectoryResponse listResponse = await schemeClient.ListDirectory(options.Database);
            listResponse.Status.EnsureSuccess();

            Console.WriteLine(listResponse.Result.Self.Name);
            foreach (var child in listResponse.Result.Children)
            {
                Console.WriteLine($"-> {child.Name}");
            }

            using TableClient tableClient = new TableClient(driver);

            {
                var readStream = tableClient.ReadTable(
                "/ru-central1/b1g63lf98jppqobv8pm2/etn03aabpli16sds88he/kv1",
                new ReadTableSettings
                {
                    Columns = new List<string> { "Value" },
                    RowLimit = 5
                });

                while (await readStream.Next())
                {
                    readStream.Response.EnsureSuccess();
                    var resultSet = readStream.Response.Result.ResultSet;

                    Console.WriteLine($"ReadTable, " +
                        $"columns: {resultSet.Columns.Count}, " +
                        $"rows: {resultSet.Rows.Count}, " +
                        $"truncated: {resultSet.Truncated}");

                    foreach (var row in resultSet.Rows)
                    {
                        Console.Write("-> ");
                        for (int columnIdx = 0; columnIdx < resultSet.Columns.Count; ++columnIdx)
                        {
                            Console.Write($"{resultSet.Columns[columnIdx].Name}: {row[columnIdx]} ");
                        }
                        Console.WriteLine();
                    }
                }
            }

            {
                var alterOperation = await tableClient.AddIndex("/ru-central1/b1g63lf98jppqobv8pm2/etn03aabpli16sds88he/kv1",
                new AddIndexSettings
                {
                    Name = "idx1",
                    IndexColumns = { "Value" }
                });

                Console.WriteLine($"Add Index" +
                    $", operation id: {alterOperation.Id}" +
                    $", ready: {alterOperation.IsReady}");

                if (!alterOperation.IsReady)
                {
                    alterOperation = await alterOperation.PollReady();
                    alterOperation.Status.EnsureSuccess();
                }
            }

            {
                ulong opKey = (ulong)new Random().Next(1, 9);
                Value.ResultSet? resultSet = null;

                var response = await tableClient.SessionExec(async session =>
                {
                    var response = await session.ExecuteDataQuery(
                        @"DECLARE $key AS Uint64;
                          SELECT Key, Value FROM kv1 WHERE Key = $key;",
                        TxControl.BeginSerializableRW(),

                        new Dictionary<string, YdbValue>
                        {
                            { "$key", YdbValue.MakeUint64(opKey) }
                        },
                        new ExecuteDataQuerySettings
                        {
                            TransportTimeout = TimeSpan.FromSeconds(1),
                            OperationTimeout = TimeSpan.FromMilliseconds(100)
                        });

                    if (!response.Status.IsSuccess || response.Tx is null)
                    {
                        return response;
                    }

                    resultSet = response.Result.ResultSets[0];

                    return await session.ExecuteDataQuery(
                        @"DECLARE $key AS Uint64;
                          DECLARE $value AS String;
                          UPSERT INTO kv1 (Key, Value) VALUES ($key, $value)",
                        TxControl.Tx(response.Tx).Commit(),
                        new Dictionary<string, YdbValue>
                        {
                            { "$key", YdbValue.MakeUint64(opKey) },
                            { "$value", YdbValue.MakeString(Encoding.ASCII.GetBytes(DateTime.Now.ToString())) }
                        },
                        new ExecuteDataQuerySettings
                        {
                            TransportTimeout = TimeSpan.FromSeconds(1),
                            OperationTimeout = TimeSpan.FromMilliseconds(100)
                        });
                });

                response.Status.EnsureSuccess();
                var queryResponse = (ExecuteDataQueryResponse)response;
                Debug.Assert(queryResponse.Tx is null);

                Debug.Assert(resultSet != null);
                Console.WriteLine($"ExecuteDataQuery, " +
                    $"columns: {resultSet.Columns.Count}, " +
                    $"rows: {resultSet.Rows.Count}, " +
                    $"truncated: {resultSet.Truncated}");

                foreach (var row in resultSet.Rows)
                {
                    var key = (ulong?)row["Key"];
                    var value = (byte[]?)row["Value"];

                    Console.WriteLine($"-> " +
                        $"Key: {key}, " +
                        $"Value: {(value != null ? Encoding.ASCII.GetString(value) : "null")}");
                }
            }

            {
                var scanStream = tableClient.ExecuteScanQuery(
                    @"DECLARE $key AS Uint64;
                      SELECT Key FROM kv1 WHERE Key < $key;",
                    new Dictionary<string, YdbValue>
                    {
                        { "$key", YdbValue.MakeUint64(9) }
                    });

                while (await scanStream.Next())
                {
                    scanStream.Response.EnsureSuccess();
                    var resultSet = scanStream.Response.Result.ResultSetPart;
                    if (resultSet != null)
                    {
                        Console.WriteLine($"ScanQuery part, " +
                            $"columns: {resultSet.Columns.Count}, " +
                            $"rows: {resultSet.Rows.Count}, " +
                            $"truncated: {resultSet.Truncated}");

                        foreach (var row in resultSet.Rows)
                        {
                            var key = (ulong?)row["Key"];

                            Console.WriteLine($"-> " +
                                $"Key: {key}, ");
                        }
                    }
                }
            }
        }

        static async Task Main(string[] args)
        {
            await Parser.Default.ParseArguments<CmdOptions>(args)
                .WithParsedAsync(Run);
        }
    }
}
