using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.BulkUpdates;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.BulkUpdates;

internal class TpcInheritanceBulkUpdatesYdbFixture : TPCInheritanceBulkUpdatesFixture
{
    protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

    public override bool UseGeneratedKeys => false;
}
