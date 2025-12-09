using System;
using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public abstract class YdbTypeMapping(
    RelationalTypeMapping.RelationalTypeMappingParameters parameters,
    YdbDbType ydbDbType
) : RelationalTypeMapping(parameters), IYdbTypeMapping
{
    public YdbDbType YdbDbType { get; } = ydbDbType;

    protected YdbTypeMapping(
        Type clrType,
        YdbDbType ydbDbType,
        JsonValueReaderWriter? jsonValueReaderWriter = null
    ) : this(
        new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(clrType, jsonValueReaderWriter: jsonValueReaderWriter),
            ydbDbType.ToString()
        ), ydbDbType
    )
    {
    }

    protected override void ConfigureParameter(DbParameter parameter)
    {
        if (parameter is not YdbParameter ydbParameter)
        {
            throw new InvalidOperationException(
                $"Ydb-specific type mapping {GetType().Name} being used with non-Ydb parameter type {parameter.GetType().Name}");
        }

        base.ConfigureParameter(parameter);
        ydbParameter.YdbDbType = YdbDbType;
    }
}
