using Microsoft.EntityFrameworkCore.Metadata;

namespace Ef.Ydb.Metadata.Internal;

public class YdbAnnotationProvider : RelationalAnnotationProvider
{
    public YdbAnnotationProvider(
        RelationalAnnotationProviderDependencies dependencies
    ) : base(dependencies)
    {
    }
}
