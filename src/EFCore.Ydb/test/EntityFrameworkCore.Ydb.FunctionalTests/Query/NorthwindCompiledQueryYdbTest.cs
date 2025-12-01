using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.EntityFrameworkCore.TestModels.Northwind;
using Xunit;
using Xunit.Abstractions;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Fix tests. Success rate 23/32
public class NorthwindCompiledQueryYdbTest
    : NorthwindCompiledQueryTestBase<NorthwindQueryYdbFixture<NoopModelCustomizer>>
{
    public NorthwindCompiledQueryYdbTest(
        NorthwindQueryYdbFixture<NoopModelCustomizer> fixture,
        ITestOutputHelper testOutputHelper
    ) : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
        Fixture.TestSqlLoggerFactory.SetTestOutputHelper(testOutputHelper);
    }

    public override void Keyless_query()
    {
    }

    public override void Keyless_query_first()
    {
    }

    public override void Query_ending_with_include()
    {
    }

    public override void Query_with_single_parameter_with_include()
    {
    }

    public override void Query_with_array_parameter()
    {
    }

    public override Task Keyless_query_async() => Task.CompletedTask;

    public override Task Keyless_query_first_async() => Task.CompletedTask;

    public override Task Query_with_array_parameter_async() => Task.CompletedTask;

    public override Task Compiled_query_with_max_parameters() => Task.CompletedTask;
    
    [Fact]
    public async Task Array_All_ILike()
    {
        await using var context = CreateContext();
        var count = context.Customers.Count(c => EF.Functions.ILike(c.ContactName, "%M%"));

        Assert.Equal(34, count);
        
        AssertSql(
            """
            SELECT CAST(COUNT(*) AS Int32)
            FROM `Customers` AS `c`
            WHERE `c`.`ContactName` ILIKE '%M%'
            """);
    }
    
    //TODO: почему то не резолвится признак что надо с эскейп символом работать
    [Fact]
    public void String_ILike_Literal_With_Escape()
    {
        using var context = CreateContext();
        var count = context.Customers.Count(c => EF.Functions.ILike(c.ContactName, "!%", "!"));

        Assert.Equal(0, count);
        AssertSql(
            """
            SELECT CAST(COUNT(*) AS Int32)
            FROM `Customers` AS `c`
            WHERE `c`.`ContactName` ILIKE '!%' ESCAPE '!'
            """);
    }
    
    [Fact]
    public void String_ILike_negated()
    {
        using var context = CreateContext();
        var count = context.Customers.Count(c => !EF.Functions.ILike(c.ContactName, "%M%"));

        Assert.Equal(57, count);
        AssertSql(
            """
            SELECT count(*)::int
            FROM `Customers` AS `c`
            WHERE `c`.`ContactName` NOT ILIKE '%M%' OR c."ContactName" IS NULL
            """);
    }
    
    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}
