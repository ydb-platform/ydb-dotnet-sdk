using EntityFrameworkCore.Ydb.Infrastructure;
using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

public static class YdbDbContextOptionsBuilderExtensions
{
    
    public static YdbDbContextOptionsBuilder ApplyConfiguration(this YdbDbContextOptionsBuilder optionsBuilder)
    {
        optionsBuilder.UseQuerySplittingBehavior(QuerySplittingBehavior.SingleQuery);
        optionsBuilder.CommandTimeout(YdbTestStore.CommandTimeout);

        return optionsBuilder;
    }
    
}
