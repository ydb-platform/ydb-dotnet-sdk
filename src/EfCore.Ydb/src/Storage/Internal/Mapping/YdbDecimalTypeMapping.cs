using System;
using System.Data.Common;
using System.Reflection;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Storage.Internal.Mapping;

public class YdbDecimalTypeMapping : RelationalTypeMapping
{
    public YdbDecimalTypeMapping(Type? type) : this(
        new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(type ?? typeof(decimal)),
            storeType: "decimal",
            dbType: System.Data.DbType.Decimal
        )
    )
    {
    }

    protected YdbDecimalTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new YdbDecimalTypeMapping(parameters);

    protected override string ProcessStoreType(
        RelationalTypeMappingParameters parameters, string storeType, string storeTypeNameBase
    )
    {
        if (storeType == "BigInteger" && parameters.Precision != null)
        {
            return $"DECIMAL({parameters.Precision}, 0)";
        }
        else
        {
            return parameters.Precision is null
                ? storeType
                : parameters.Scale is null
                    ? $"DECIMAL({parameters.Precision}, 0)"
                    : $"DECIMAL({parameters.Precision},{parameters.Scale})";
        }
    }

    public override MethodInfo GetDataReaderMethod()
    {
        return typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetDecimal), [typeof(int)])!;
    }
}
