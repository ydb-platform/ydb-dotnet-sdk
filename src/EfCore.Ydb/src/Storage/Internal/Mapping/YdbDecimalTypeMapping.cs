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
            storeType: "Decimal",
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
    ) => storeType == "BigInteger" && parameters.Precision != null
        ? $"Decimal({parameters.Precision}, 0)"
        : parameters.Precision is null
            ? storeType
            : parameters.Scale is null
                ? $"Decimal({parameters.Precision}, 0)"
                : $"Decimal({parameters.Precision}, {parameters.Scale})";

    public override MethodInfo GetDataReaderMethod() =>
        typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetDecimal), [typeof(int)])!;
}
