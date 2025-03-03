using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestStore : RelationalTestStore
{
    public const int CommandTimeout = 600;
    private readonly string? _scriptPath;
    private readonly string? _additionalSql;

    public YdbTestStore(
        string name,
        string? scriptPath = null,
        string? additionalSql = null,
        string? connectionStringOptions = null,
        bool shared = true,
        bool useConnectionString = false
    ) : base(name, shared, CreateConnection())
    {
        _scriptPath = scriptPath;
        _additionalSql = additionalSql;
    }

    public static YdbTestStore GetOrCreate(
        string name,
        string? scriptPath = null
    ) => new(name: name, scriptPath: scriptPath);

    public override DbContextOptionsBuilder AddProviderOptions(DbContextOptionsBuilder builder)
    {
        return UseConnectionString
            ? builder.UseEfYdb(Connection.ConnectionString)
            : builder.UseEfYdb(Connection);
    }

    protected override async Task InitializeAsync(
        Func<DbContext> createContext, Func<DbContext, Task>? seed, Func<DbContext, Task>? clean
    )
    {
        if (_scriptPath is not null)
        {
            await ExecuteScript(_scriptPath);

            if (_additionalSql is not null)
            {
                await ExecuteAsync(Connection, command => command.ExecuteNonQueryAsync(), _additionalSql);
            }
        }
        else
        {
            await using var context = createContext();
            await context.Database.EnsureCreatedResilientlyAsync();

            if (_additionalSql is not null)
            {
                await ExecuteAsync(Connection, command => command.ExecuteNonQueryAsync(), _additionalSql);
            }

            if (seed is not null)
            {
                await seed(context);
            }
        }
    }


    public async Task ExecuteScript(string scriptPath)
    {
        var script = await File.ReadAllTextAsync(scriptPath);
        await ExecuteAsync(
            Connection, command =>
            {
                var commandsToExecute =
                    new Regex("^GO",
                            RegexOptions.IgnoreCase | RegexOptions.Multiline,
                            TimeSpan.FromMilliseconds(1000.0)
                        )
                        .Split(script)
                        .Where(b => !string.IsNullOrEmpty(b));

                var commandToExecutes = commandsToExecute.ToList();
                foreach (var commandToExecute in commandToExecutes)
                {
                    try
                    {
                        var commandsSplitted = new Regex(
                                "\n",
                                RegexOptions.IgnoreCase | RegexOptions.Multiline,
                                TimeSpan.FromMilliseconds(1_000)
                            )
                            .Split(commandToExecute)
                            .Where(b => !b.StartsWith("--") && !string.IsNullOrEmpty(b))
                            .ToList();

                        var readyCommand = string.Join("\n", commandsSplitted);

                        command.CommandTimeout = 100_000;
                        command.CommandText = readyCommand;
                        command.ExecuteNonQueryAsync();
                    }
                    catch (Exception e)
                    {
                        throw new AggregateException($"Exception for command:\n{commandToExecute}\n", e);
                    }
                }

                return Task.FromResult(0);
            }, "");
    }

    private static async Task ExecuteAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> execute,
        string sql,
        bool useTransaction = false,
        object[]? parameters = null
    )
    {
        await ExecuteCommandAsync(connection, execute, sql, useTransaction, parameters);
    }

    private static async Task ExecuteCommandAsync<T>(
        DbConnection connection,
        Func<DbCommand, Task<T>> execute,
        string sql,
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
            if (connection.State == ConnectionState.Closed
                && connection.State != ConnectionState.Closed)
            {
                await connection.CloseAsync();
            }
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

        if (parameters is not null)
        {
            for (var i = 0; i < parameters.Count; i++)
            {
                command.Parameters.AddWithValue("p" + i, parameters[i]);
            }
        }

        return command;
    }


    private static YdbConnection CreateConnection()
    {
        return new YdbConnection();
    }

    public override Task CleanAsync(DbContext context)
    {
        return Task.CompletedTask;
    }
}
