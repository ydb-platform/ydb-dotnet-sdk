using Microsoft.EntityFrameworkCore.TestUtilities;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class QueryNoClientEvalYdbFixture : NorthwindQueryYdbFixture<NoopModelCustomizer>;
