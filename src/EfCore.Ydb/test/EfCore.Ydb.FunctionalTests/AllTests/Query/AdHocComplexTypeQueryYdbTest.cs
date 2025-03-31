using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.AllTests.Query;

public class AdHocComplexTypeQueryYdbTest : AdHocComplexTypeQueryTestBase
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
