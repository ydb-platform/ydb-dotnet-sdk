using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

// TODO: Requires Time type 
internal class GearsOfWarQueryYdbTest(GearsOfWarQueryYdbFixture fixture)
    : GearsOfWarQueryRelationalTestBase<GearsOfWarQueryYdbFixture>(fixture);