using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

// TODO: Expects actual decimal precision. But ydb supports only (22, 9)
internal class ConvertToProviderTypesYdbTest : ConvertToProviderTypesTestBase<
    ConvertToProviderTypesYdbTest.ConvertToProviderTypesYdbFixture>
{
    public ConvertToProviderTypesYdbTest(ConvertToProviderTypesYdbFixture fixture) : base(fixture)
    {
        Fixture.TestSqlLoggerFactory.Clear();
    }

    public class ConvertToProviderTypesYdbFixture : ConvertToProviderTypesFixtureBase
    {
        public override bool StrictEquality
            => true;

        public override bool SupportsAnsi
            => false;

        public override bool SupportsUnicodeToAnsiConversion
            => false;

        public override bool SupportsLargeStringComparisons
            => true;

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        public TestSqlLoggerFactory TestSqlLoggerFactory
            => (TestSqlLoggerFactory)ServiceProvider.GetRequiredService<ILoggerFactory>();

        public override bool SupportsBinaryKeys
            => true;

        public override bool SupportsDecimalComparisons
            => true;

        public override DateTime DefaultDateTime
            => new();

        public override bool PreservesDateTimeKind
            => false;

    }
}
