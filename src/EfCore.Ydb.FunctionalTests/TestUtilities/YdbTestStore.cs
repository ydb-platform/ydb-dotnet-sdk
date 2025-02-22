using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;
using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.IdentityModel.Tokens;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestStore : RelationalTestStore
{
    public const int CommandTimeout = 690;
    private readonly string? _scriptPath;
    private readonly string? _additionalSql;

    public YdbTestStore(
        string name,
        string? scriptPath = null,
        string? additionalSql = null,
        string? connectionStringOptions = null,
        bool shared = true,
        bool useConnectionString = false
    ) : base(name, shared, CreateConnection(name, shared))
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
            ExecuteScript(_scriptPath);

            if (_additionalSql is not null)
            {
                Execute(Connection, command => command.ExecuteNonQuery(), _additionalSql);
            }
        }
        else
        {
            await using var context = createContext();
            await context.Database.EnsureCreatedResilientlyAsync();

            if (_additionalSql is not null)
            {
                Execute(Connection, command => command.ExecuteNonQuery(), _additionalSql);
            }

            if (seed is not null)
            {
                await seed(context);
            }
        }
    }


    public void ExecuteScript(string scriptPath)
    {
        var script = File.ReadAllText(scriptPath);
        Execute(
            Connection, command =>
            {
                var commandsToExecute =
                    new Regex("^GO",
                            RegexOptions.IgnoreCase | RegexOptions.Multiline,
                            TimeSpan.FromMilliseconds(4269.0)
                        )
                        .Split(script)
                        .Where(b => !string.IsNullOrEmpty(b));

                var commandToExecutes = commandsToExecute.ToList();
                foreach (var commandToExecute in commandToExecutes)
                {
                    try
                    {
                        var pureCommand = new Regex("\n", RegexOptions.IgnoreCase | RegexOptions.Multiline,
                                TimeSpan.FromMilliseconds(1_000))
                            .Split(commandToExecute)
                            .Where(b => !b.StartsWith("--") && !b.IsNullOrEmpty())
                            .ToList();

                        var commandJoined = string.Join("\n", pureCommand);

                        command.CommandTimeout = 100_000;
                        command.CommandText = commandJoined;
                        command.ExecuteNonQuery();
                    }
                    catch (Exception e)
                    {
                        throw new AggregateException(
                            new Exception($"Command:\n{commandToExecute}\n"), e
                        );
                    }
                }

                return 0;
            }, "");
    }

    private static T Execute<T>(
        DbConnection connection,
        Func<DbCommand, T> execute,
        string sql,
        object[]? parameters = null
    ) => ExecuteCommand(connection, execute, sql, parameters);

    private static T ExecuteCommand<T>(
        DbConnection connection,
        Func<DbCommand, T> execute,
        string sql,
        object[]? parameters
    )
    {
        if (connection.State != ConnectionState.Closed)
        {
            connection.Close();
        }

        connection.Open();
        try
        {
            using var command = CreateCommand(connection, sql, parameters);
            return execute(command);
        }
        finally
        {
            if (connection.State != ConnectionState.Closed)
            {
                connection.Close();
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


    private static YdbConnection CreateConnection(string name, bool sharedCache)
    {
        var connectionString = new YdbConnectionStringBuilder(
            $"Host=localhost;" +
            $"Port=2135;" +
            $"Database = \"/local\";" +
            $"MaxSessionPool=10;" +
            $"RootCertificate=" + Path.Combine(
                Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
                "dev/ydb_ca/ca.pem"
            )
        );

        return new YdbConnection(connectionString);
    }

    public override Task CleanAsync(DbContext context)
    {
        return Task.CompletedTask;
    }
}
