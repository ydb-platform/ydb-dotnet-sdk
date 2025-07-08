using Microsoft.EntityFrameworkCore.Storage;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbGuidTypeMapping : GuidTypeMapping
{
    public new static YdbGuidTypeMapping Default => new();

    private YdbGuidTypeMapping() : base("Uuid", System.Data.DbType.Guid)
    {
    }

    protected YdbGuidTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
        new YdbGuidTypeMapping(parameters);

    protected override string SqlLiteralFormatString => "Uuid('{0}')";
}
