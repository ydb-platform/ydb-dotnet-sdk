using System.Data;
using System.Data.Common;
using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Xunit;
using Ydb.Sdk;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class YdbExecutionStrategyTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Query_WhenSessionBreaksBeforeExecution_Retries(bool async)
    {
        var interceptor = new BreakSessionInterceptor();
        var options = new DbContextOptionsBuilder<DbContext>()
            .UseYdb("Host=localhost;Port=2136;Database=/local", ydb => ydb.UseRetryPolicy(
                new YdbRetryPolicyConfig { MaxAttempts = 2 }))
            .AddInterceptors(interceptor)
            .Options;
        await using var context = new DbContext(options);
        var query = context.Database.SqlQueryRaw<int>("SELECT 42 AS Value");

        var values = async ? await query.ToListAsync() : query.ToList();

        Assert.Equal(42, Assert.Single(values));
        Assert.Equal(2, interceptor.Attempts);
    }

    private sealed class BreakSessionInterceptor : DbCommandInterceptor
    {
        public int Attempts { get; private set; }

        public override InterceptionResult<DbDataReader> ReaderExecuting(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<DbDataReader> result)
        {
            if (++Attempts == 1)
            {
                var connection = (YdbConnection)command.Connection!;
                Assert.Equal(ConnectionState.Open, connection.State);
                connection.OnNotSuccessStatusCode(StatusCode.BadSession);
                Assert.Equal(ConnectionState.Broken, connection.State);
            }

            return result;
        }

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command,
            CommandEventData eventData,
            InterceptionResult<DbDataReader> result,
            CancellationToken cancellationToken = default) =>
            new(ReaderExecuting(command, eventData, result));
    }
}
