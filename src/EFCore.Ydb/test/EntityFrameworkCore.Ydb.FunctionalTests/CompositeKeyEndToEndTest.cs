using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class CompositeKeyEndToEndYdbTest(CompositeKeyEndToEndYdbTest.CompositeKeyEndToEndYdbFixture fixture)
    : CompositeKeyEndToEndTestBase<CompositeKeyEndToEndYdbTest.CompositeKeyEndToEndYdbFixture>(fixture)
{
    
    [ConditionalFact(Skip = "TODO: Cannot access table")]
    public override Task Can_use_generated_values_in_composite_key_end_to_end()
        => base.Can_use_generated_values_in_composite_key_end_to_end();

    public class CompositeKeyEndToEndYdbFixture : CompositeKeyEndToEndFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
