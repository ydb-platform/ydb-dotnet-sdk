using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class KeysWithConvertersYdbTest(KeysWithConvertersYdbTest.KeysWithConvertersYdbFixture fixture)
    : KeysWithConvertersTestBase<
        KeysWithConvertersYdbTest.KeysWithConvertersYdbFixture>(fixture)
{
    public class KeysWithConvertersYdbFixture : KeysWithConvertersFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
            => builder.UseYdb("");
    }
}
