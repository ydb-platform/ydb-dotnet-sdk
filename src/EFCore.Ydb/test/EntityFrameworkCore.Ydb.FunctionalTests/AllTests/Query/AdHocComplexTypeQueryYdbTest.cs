using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.AllTests.Query;

public class AdHocComplexTypeQueryYdbTest : AdHocComplexTypeQueryTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
