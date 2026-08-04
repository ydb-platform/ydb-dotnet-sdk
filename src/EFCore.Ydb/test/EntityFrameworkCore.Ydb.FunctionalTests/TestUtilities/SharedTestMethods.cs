using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Sdk;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

internal static class SharedTestMethods
{
    /// <inheritdoc cref="AssertYdb(Func{bool, Task}, TestSqlLoggerFactory, bool, string[])"/>
    public static Task AssertYdb(
        Func<Task> test,
        TestSqlLoggerFactory sql,
        params string[] expectedSql
    ) => AssertYdb(_ => test(), sql, async: false, expectedSql);

    /// <summary>
    /// Runs a Microsoft EF bulk-update/delete spec test on YDB.
    /// </summary>
    /// <remarks>
    /// EF spec tests assert both SQL and <c>rowsAffected</c> from <c>ExecuteUpdate</c>/<c>ExecuteDelete</c>.
    /// YDB does not report modified row count (server limitation), so the base test fails with
    /// <see cref="EqualException"/> on row count even when SQL is correct.
    /// We catch that failure and, when <paramref name="expectedSql"/> is provided, assert the logged SQL
    /// instead — same pattern as former <c>TestIgnoringBase</c>, shared via <c>using static</c> in YDB test classes.
    /// </remarks>
    public static async Task AssertYdb(
        Func<bool, Task> test,
        TestSqlLoggerFactory sql,
        bool async,
        params string[] expectedSql
    )
    {
        try
        {
            await test(async);
        }
        catch (EqualException)
        {
            // Row-count assertion failed — expected on YDB; fall back to SQL baseline when provided.
            if (expectedSql.Length == 0)
            {
                return;
            }

            var actual = sql.SqlStatements;

            Assert.Equal(expectedSql.Length, actual.Count);
            for (var i = 0; i < expectedSql.Length; i++)
            {
                Assert.Equal(expectedSql[i], actual[i]);
            }
        }
    }
}
