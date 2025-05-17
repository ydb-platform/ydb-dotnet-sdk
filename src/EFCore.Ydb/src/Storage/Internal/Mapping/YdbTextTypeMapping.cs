using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbTextTypeMapping : RelationalTypeMapping
{
    public static YdbTextTypeMapping Default { get; } = new("Text");

    public YdbTextTypeMapping(string storeType)
        : base(
            new RelationalTypeMappingParameters(
                new CoreTypeMappingParameters(
                    typeof(string),
                    jsonValueReaderWriter: JsonStringReaderWriter.Instance
                ),
                storeType: storeType,
                storeTypePostfix: StoreTypePostfix.None,
                dbType: System.Data.DbType.String,
                unicode: true
            )
        )
    {
    }

    protected YdbTextTypeMapping(RelationalTypeMappingParameters parameters)
        : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new YdbTextTypeMapping(parameters);

    protected virtual string EscapeSqlLiteral(string literal) => literal.Replace("'", "\\'");

    protected override string GenerateNonNullSqlLiteral(object value) => $"'{EscapeSqlLiteral((string)value)}'u";
}
