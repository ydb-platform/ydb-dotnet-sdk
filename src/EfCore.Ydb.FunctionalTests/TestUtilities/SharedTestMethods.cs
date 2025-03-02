using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;
using Xunit.Sdk;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

static class SharedTestMethods
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
        catch (EqualException e)
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
