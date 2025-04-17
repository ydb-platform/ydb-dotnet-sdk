using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Ydb.Sdk.Ado;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestStore(
    string name,
    string? scriptPath = null,
    string? additionalSql = null
) : RelationalTestStore(name, false, CreateConnection())
{
    private const int CommandTimeout = 6942;

    internal Task ExecuteNonQueryAsync(string sql, params object[] parameters)
        => ExecuteAsync(Connection, command => command.ExecuteNonQueryAsync(), sql, false, parameters);

    public static YdbTestStore GetOrCreate(
        string name,
        string? scriptPath = null
    ) => new(name: name, scriptPath: scriptPath);

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder) => UseConnectionString
        ? builder.UseYdb(Connection.ConnectionString)
        : builder.UseYdb(Connection);

    protected override async Task InitializeAsync(
        Func<DbContext> createContext,
        Func<DbContext, Task>? seed,
        Func<DbContext, Task>? clean
    )
    {
        await using var context = createContext();
        if (clean != null) await clean(context);
        await CleanAsync(context);
        if (scriptPath is not null)
        {
            await ExecuteScript(scriptPath);

            if (additionalSql is not null)
            {
                await ExecuteAsync(Connection, command => command.ExecuteNonQueryAsync(), additionalSql);
            }
        }
        else
        {
            await context.Database.EnsureCreatedAsync();

            if (additionalSql is not null)
            {
                await ExecuteAsync(Connection, command => command.ExecuteNonQueryAsync(), additionalSql);
            }

            if (seed is not null)
            {
                await seed(context);
            }
        }
    }

    private async Task ExecuteScript(string scriptPathParam)
    {
        var script = await File.ReadAllTextAsync(scriptPathParam);
        await ExecuteAsync(
            Connection, async command =>
            {
                var commandsToExecute =
                    new Regex("^GO",
                            RegexOptions.IgnoreCase | RegexOptions.Multiline,
                            TimeSpan.FromMilliseconds(10_000.0)
                        )
                        .Split(script)
                        .Where(b => !string.IsNullOrEmpty(b));

                var commandToExecutes = commandsToExecute.ToList();
                foreach (var commandToExecute in commandToExecutes)
                {
                    try
                    {
                        var commandsSplit = new Regex(
                                "\n",
                                RegexOptions.IgnoreCase | RegexOptions.Multiline,
                                TimeSpan.FromMilliseconds(10_000)
                            )
                            .Split(commandToExecute)
                            .Where(b => !b.StartsWith("--") && !string.IsNullOrEmpty(b))
                            .ToList();

                        var readyCommand = string.Join("\n", commandsSplit);

                        command.CommandTimeout = 100_000;
                        command.CommandText = readyCommand;
                        await command.ExecuteNonQueryAsync();
                    }
                    catch (Exception e)
                    {
                        throw new AggregateException($"Exception for command:\n{commandToExecute}\n", e);
                    }
                }

                return 0;
            }, "");
    }

    private static async Task ExecuteAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> execute,
        string sql,
        bool useTransaction = false,
        object[]? parameters = null
    ) => await ExecuteCommandAsync(connection, execute, sql, useTransaction, parameters);

    private static async Task ExecuteCommandAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> execute,
        string sql,
        // ReSharper disable once UnusedParameter.Local
        bool useTransaction,
        object[]? parameters
    )
    {
        if (connection.State != ConnectionState.Closed)
        {
            await connection.CloseAsync();
        }

        await connection.OpenAsync();
        try
        {
            await using var command = CreateCommand(connection, sql, parameters);
            await execute(command);
        }
        finally
        {
            await connection.CloseAsync();
        }
    }

    private static YdbCommand CreateCommand(
        DbConnection connection,
        string commandText,
        IReadOnlyList<object>? parameters = null
    )
    {
        var command = (YdbCommand)connection.CreateCommand();

        command.CommandText = commandText;
        command.CommandTimeout = CommandTimeout;

        if (parameters is null)
        {
            return command;
        }

        for (var i = 0; i < parameters.Count; i++)
        {
            command.Parameters.AddWithValue("p" + i, parameters[i]);
        }

        return command;
    }


    private static YdbConnection CreateConnection() => new(new YdbConnectionStringBuilder { MaxSessionPool = 10 });

    public override async Task CleanAsync(DbContext context)
    {
        var connection = context.Database.GetDbConnection();
        if (connection.State != ConnectionState.Open)
        {
            await connection.OpenAsync();
        }

        var schema = await connection.GetSchemaAsync("tables");
        var tables = schema
            .AsEnumerable()
            .Select(entry => (string)entry["table_name"])
            .Where(tableName => !tableName.StartsWith('.'));

        if (!tables.Any()) return;

        var command = connection.CreateCommand();

        foreach (var table in tables)
        {
            command.CommandText += $"DROP TABLE IF EXISTS `{table}`;";
        }

        await command.ExecuteNonQueryAsync();

        await connection.CloseAsync();
    }

    protected override string OpenDelimiter => "`";
    protected override string CloseDelimiter => "`";
}
