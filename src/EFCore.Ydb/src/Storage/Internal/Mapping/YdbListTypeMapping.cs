using System.Collections;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

internal class YdbListTypeMapping() : RelationalTypeMapping(
    new RelationalTypeMappingParameters(
        new CoreTypeMappingParameters(typeof(IList)),
        storeType: "List",
        dbType: System.Data.DbType.Object
    )
)
{
    internal static readonly YdbListTypeMapping Default = new();

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) => this;
}
