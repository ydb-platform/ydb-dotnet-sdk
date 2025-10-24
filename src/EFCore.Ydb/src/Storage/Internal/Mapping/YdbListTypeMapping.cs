using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbListTypeMapping() : RelationalTypeMapping(new RelationalTypeMappingParameters())
{
    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => this;
}
