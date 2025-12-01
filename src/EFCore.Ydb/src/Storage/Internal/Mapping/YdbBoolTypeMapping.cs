using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbBoolTypeMapping : BoolTypeMapping
{
    public new static YdbBoolTypeMapping Default { get; } = new();

    private YdbBoolTypeMapping() : base("Bool")
    {
    }

    private YdbBoolTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override YdbBoolTypeMapping Clone(RelationalTypeMappingParameters parameters) => new(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
        => (bool)value ? "TRUE" : "FALSE";
}
