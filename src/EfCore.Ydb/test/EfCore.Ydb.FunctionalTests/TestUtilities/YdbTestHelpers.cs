using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.FunctionalTests.TestUtilities;

public class YdbTestHelpers : RelationalTestHelpers
{
    private YdbTestHelpers()
    {
    }

    public static YdbTestHelpers Instance { get; } = new();

    public override IServiceCollection AddProviderServices(IServiceCollection services) =>
        services.AddEntityFrameworkYdb();

    public override DbContextOptionsBuilder UseProviderOptions(DbContextOptionsBuilder optionsBuilder) =>
        optionsBuilder.UseYdb(new YdbConnection("Port=2136;Host=localhost;Database=/local"));
}
