using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbDecimalTypeMapping : DecimalTypeMapping
{
    private const byte DefaultPrecision = 22;
    private const byte DefaultScale = 9;

    public new static YdbDecimalTypeMapping Default => new();

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
}
