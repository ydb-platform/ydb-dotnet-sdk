using EfCore.Ydb.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace EfCore.Ydb.Infrastructure;

public class YdbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    : RelationalDbContextOptionsBuilder<YdbDbContextOptionsBuilder, YdbOptionsExtension>(optionsBuilder);
