using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Sdk;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

internal static class SharedTestMethods
{
    public static async Task TestIgnoringBase(
        Func<Task> baseTest,
        TestSqlLoggerFactory loggerFactory,
        params string[] expectedSql
    ) => await TestIgnoringBase(_ => baseTest(), loggerFactory, false, expectedSql);

    public static async Task TestIgnoringBase(
        Func<bool, Task> baseTest,
        TestSqlLoggerFactory loggerFactory,
        bool async,
        params string[] expectedSql
    )
    {
        try
        {
            await baseTest(async);
        }
        catch (EqualException)
        {
            var actual = loggerFactory.SqlStatements;

            Assert.Equal(expectedSql.Length, actual.Count);
            for (var i = 0; i < expectedSql.Length; i++)
            {
                Assert.Equal(expectedSql[i], actual[i]);
            }
        }
    }
}
