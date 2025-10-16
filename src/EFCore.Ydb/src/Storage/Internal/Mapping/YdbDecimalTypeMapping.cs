using System.Data.Common;
using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbDecimalTypeMapping : DecimalTypeMapping
{
    private const byte DefaultPrecision = 22;
    private const byte DefaultScale = 9;

    private const byte MaxPrecision = 35;

    public new static YdbDecimalTypeMapping Default => new();

    public static YdbDecimalTypeMapping GetWithMaxPrecision(int? scale) =>
        new(new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(decimal)),
            storeType: "Decimal",
            dbType: System.Data.DbType.Decimal,
            precision: MaxPrecision,
            scale: scale ?? DefaultScale)
        );

    public YdbDecimalTypeMapping() : this(
        new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(typeof(decimal)),
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
    ) => $"Decimal({parameters.Precision ?? DefaultPrecision}, {parameters.Scale ?? DefaultScale})";

    protected override void ConfigureParameter(DbParameter parameter)
    {
        base.ConfigureParameter(parameter);

        if (Precision is { } p)
            parameter.Precision = (byte)p;

        if (Scale is { } s)
            parameter.Scale = (byte)s;
    }

    protected override string GenerateNonNullSqlLiteral(object value) =>
        $"Decimal('{base.GenerateNonNullSqlLiteral(value)}', {Precision ?? DefaultPrecision}, {Scale ?? DefaultScale})";
}
