using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore.Query;
using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class GearsOfWarQueryYdbFixture : GearsOfWarQueryRelationalFixture
{
    protected override string StoreName
        => "GearsOfWarQueryTest";
    
    protected override ITestStoreFactory TestStoreFactory
        => YdbTestStoreFactory.Instance;
}
