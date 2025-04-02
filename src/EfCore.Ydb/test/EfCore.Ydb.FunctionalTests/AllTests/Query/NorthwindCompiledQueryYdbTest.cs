using EfCore.Ydb.FunctionalTests.Query;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit.Abstractions;

namespace EfCore.Ydb.FunctionalTests.AllTests.Query;

// TODO: Fix tests
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
}
