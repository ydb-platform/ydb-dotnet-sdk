using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.AllTests.BulkUpdates;

public class TPCInheritanceBulkUpdatesYdbFixture : TPCInheritanceBulkUpdatesFixture
{
    protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

    public override bool UseGeneratedKeys => false;
}
