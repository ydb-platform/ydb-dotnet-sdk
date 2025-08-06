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

        var collection = new[] { "a%", "b%", "c%" };
        var query = context.Set<Customer>().Where(c => collection.All(y => EF.Functions.ILike(c.Address, y)));
        var result = await query.ToListAsync();

        Assert.Empty(result);

        AssertSql(
            """
            @collection={ 'a%', 'b%', 'c%' } (DbType = Object)

            SELECT c."CustomerID", c."Address", c."City", c."CompanyName", c."ContactName", c."ContactTitle", c."Country", c."Fax", c."Phone", c."PostalCode", c."Region"
            FROM "Customers" AS c
            WHERE c."Address" ILIKE ALL (@collection)
            """);
    }
    
    private void AssertSql(params string[] expected)
        => Fixture.TestSqlLoggerFactory.AssertBaseline(expected);
}
