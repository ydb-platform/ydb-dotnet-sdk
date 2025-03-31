using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestHelpers : RelationalTestHelpers
{
    public static YdbTestHelpers Instance { get; } = new();

    public override IServiceCollection AddProviderServices(IServiceCollection services)
        => services.AddEntityFrameworkYdb();

    public override DbContextOptionsBuilder UseProviderOptions(DbContextOptionsBuilder optionsBuilder)
        => optionsBuilder.UseYdb(string.Empty);
}
