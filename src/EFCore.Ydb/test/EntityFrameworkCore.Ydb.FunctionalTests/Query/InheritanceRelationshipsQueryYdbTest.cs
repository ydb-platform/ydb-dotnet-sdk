using Microsoft.EntityFrameworkCore.Query;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public class InheritanceRelationshipsQueryYdbTest(InheritanceRelationshipsQueryYdbFixture fixture)
    : InheritanceRelationshipsQueryTestBase<InheritanceRelationshipsQueryYdbFixture>(fixture);