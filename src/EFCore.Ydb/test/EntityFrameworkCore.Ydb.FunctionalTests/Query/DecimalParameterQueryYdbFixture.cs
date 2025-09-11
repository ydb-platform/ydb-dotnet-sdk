using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class DecimalParameterQueryYdbFixture : SharedStoreFixtureBase<DecimalParameterQueryYdbFixture.TestContext>
{
    protected override string StoreName => "DecimalParameterTest";

    protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

    public class TestContext(DbContextOptions options) : DbContext(options);
}
