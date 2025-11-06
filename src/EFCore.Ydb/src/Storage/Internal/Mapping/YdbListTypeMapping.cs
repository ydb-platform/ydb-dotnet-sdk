using System.Collections;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

internal class YdbListTypeMapping(
    YdbDbType ydbDbType,
    string storeTypeElement
) : RelationalTypeMapping(storeType: $"List<{storeTypeElement}>", typeof(IList))
{
    protected override YdbListTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
        new(ydbDbType, storeTypeElement);

    protected override void ConfigureParameter(DbParameter parameter)
    {
    }
}
