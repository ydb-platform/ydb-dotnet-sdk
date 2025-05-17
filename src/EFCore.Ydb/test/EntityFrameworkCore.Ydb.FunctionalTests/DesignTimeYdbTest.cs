using System.Reflection;
using EntityFrameworkCore.Ydb.Design.Internal;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class DesignTimeYdbTest(DesignTimeYdbTest.DesignTimeYdbFixture fixture)
    : DesignTimeTestBase<DesignTimeYdbTest.DesignTimeYdbFixture>(fixture)
{
    protected override Assembly ProviderAssembly
        => typeof(YdbDesignTimeServices).Assembly;

    public class DesignTimeYdbFixture : DesignTimeFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
