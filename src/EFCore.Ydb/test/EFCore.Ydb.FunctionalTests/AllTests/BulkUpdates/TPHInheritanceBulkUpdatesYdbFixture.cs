using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

internal class TPHInheritanceBulkUpdatesYdbFixture : TPHInheritanceBulkUpdatesFixture
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
