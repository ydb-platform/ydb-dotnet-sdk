using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class ModelBuilding101YdbTest : ModelBuilding101RelationalTestBase
{
    protected override DbContextOptionsBuilder ConfigureContext(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseYdb("");
}
