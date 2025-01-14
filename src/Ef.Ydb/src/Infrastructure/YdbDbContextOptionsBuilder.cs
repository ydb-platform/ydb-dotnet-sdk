using Ef.Ydb.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Ef.Ydb.Infrastructure;

public class YdbDbContextOptionsBuilder
    : RelationalDbContextOptionsBuilder<YdbDbContextOptionsBuilder, YdbOptionsExtension>
{
    public YdbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder) : base(optionsBuilder)
    {
    }
}
