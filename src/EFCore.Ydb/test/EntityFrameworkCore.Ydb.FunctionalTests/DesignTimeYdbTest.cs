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
#pragma warning disable EF1001
        => typeof(YdbDesignTimeServices).Assembly;
#pragma warning restore EF1001

    public class DesignTimeYdbFixture : DesignTimeFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
