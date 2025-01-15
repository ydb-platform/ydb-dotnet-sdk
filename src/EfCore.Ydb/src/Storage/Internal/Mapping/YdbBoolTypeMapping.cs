using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Storage.Internal.Mapping;

public class YdbBoolTypeMapping : BoolTypeMapping
{
    public new static YdbBoolTypeMapping Default { get; } = new();

    public YdbBoolTypeMapping() : base("BOOL")
    {
    }

    protected YdbBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new YdbBoolTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => (bool)value ? "true" : "false";
}
