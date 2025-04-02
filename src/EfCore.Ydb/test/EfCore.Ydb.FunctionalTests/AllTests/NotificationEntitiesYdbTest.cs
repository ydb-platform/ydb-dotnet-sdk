using EfCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EfCore.Ydb.FunctionalTests.AllTests;

public class NotificationEntitiesYdbTest(NotificationEntitiesYdbTest.NotificationEntitiesYdbFixture fixture)
    : NotificationEntitiesTestBase<NotificationEntitiesYdbTest.NotificationEntitiesYdbFixture>(fixture)
{
    public class NotificationEntitiesYdbFixture : NotificationEntitiesFixtureBase
    {
        protected override string StoreName { get; } = "NotificationEntities";

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;
    }
}
