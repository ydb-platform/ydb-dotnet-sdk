using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public abstract class FindYdbTest : FindTestBase<FindYdbTest.FindYdbFixture>
{
    protected FindYdbTest(FindYdbFixture fixture)
        : base(fixture)
    {
        fixture.TestSqlLoggerFactory.Clear();
    }

    [ConditionalFact(Skip = "TODO: Requires fixes")]
    public override void Find_int_key_from_store() => base.Find_int_key_from_store();

    [ConditionalTheory(Skip = "TODO: Requires fixes")]
    [InlineData(CancellationType.None)]
    public override Task Find_int_key_from_store_async(CancellationType cancellationType) =>
        base.Find_int_key_from_store_async(cancellationType);

    [ConditionalFact(Skip = "Requires fixes")]
    public override void Returns_null_for_int_key_not_in_store() => base.Returns_null_for_int_key_not_in_store();

    [ConditionalTheory(Skip = "TODO: Requires fixes")]
    [InlineData(CancellationType.None)]
    public override Task Returns_null_for_int_key_not_in_store_async(CancellationType cancellationType) =>
        base.Returns_null_for_int_key_not_in_store_async(cancellationType);

    public class FindYdbTestSet(FindYdbFixture fixture) : FindYdbTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaSetFinder();
    }

    public class FindYdbTestContext(FindYdbFixture fixture) : FindYdbTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaContextFinder();
    }

    public class FindYdbTestNonGeneric(FindYdbFixture fixture) : FindYdbTest(fixture)
    {
        protected override TestFinder Finder { get; } = new FindViaNonGenericContextFinder();
    }

    public class FindYdbFixture : FindFixtureBase
    {
        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
