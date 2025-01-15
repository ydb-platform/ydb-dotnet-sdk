using Microsoft.EntityFrameworkCore.Metadata;

namespace EfCore.Ydb.Metadata.Internal;

public class YdbAnnotationProvider : RelationalAnnotationProvider
{
    public YdbAnnotationProvider(
        RelationalAnnotationProviderDependencies dependencies
    ) : base(dependencies)
    {
    }
}
