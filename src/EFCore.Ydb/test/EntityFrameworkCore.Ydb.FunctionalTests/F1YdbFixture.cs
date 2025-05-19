using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class F1ULongYdbFixture : F1YdbFixtureBase<ulong?>
{
    protected override string StoreName
        => "F1ULongTest";
}

public class F1YdbFixture : F1YdbFixtureBase<byte[]>;

public abstract class F1YdbFixtureBase<TRowVersion> : F1RelationalFixture<TRowVersion>
{
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;

    public override TestHelpers TestHelpers
        => YdbTestHelpers.Instance;
}
