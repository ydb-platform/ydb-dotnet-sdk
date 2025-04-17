using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

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
