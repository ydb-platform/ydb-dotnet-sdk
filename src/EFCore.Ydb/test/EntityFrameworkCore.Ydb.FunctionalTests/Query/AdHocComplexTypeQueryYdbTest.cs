using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
#if !EFCORE9
using Microsoft.EntityFrameworkCore;
#endif
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class AdHocComplexTypeQueryYdbTest : AdHocComplexTypeQueryTestBase
{
#if !EFCORE9
    public AdHocComplexTypeQueryYdbTest(NonSharedFixture fixture) : base(fixture)
    {
    }
#endif

    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
