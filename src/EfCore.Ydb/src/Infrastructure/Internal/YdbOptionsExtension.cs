using System.Collections.Generic;
using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;

namespace EfCore.Ydb.Infrastructure.Internal;

public class YdbOptionsExtension : RelationalOptionsExtension
{
    private DbContextOptionsExtensionInfo? _info;

    public YdbOptionsExtension()
    {
    }

    protected YdbOptionsExtension(YdbOptionsExtension copyFrom) : base(copyFrom)
    {
    }

    protected override RelationalOptionsExtension Clone()
        => new YdbOptionsExtension(this);

    public override void ApplyServices(IServiceCollection services)
        => services.AddEntityFrameworkYdb();

    public override DbContextOptionsExtensionInfo Info
        => _info ??= new ExtensionInfo(this);

    private sealed class ExtensionInfo(IDbContextOptionsExtension extension) : RelationalExtensionInfo(extension)
    {
        private new YdbOptionsExtension Extension => (YdbOptionsExtension)base.Extension;

        public override bool IsDatabaseProvider => true;

        // TODO: Right now it's stub
        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) => debugInfo["Hello"] = "World!";
    }
}
