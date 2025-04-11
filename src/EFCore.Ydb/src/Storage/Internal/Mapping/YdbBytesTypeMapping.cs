using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbBytesTypeMapping : RelationalTypeMapping
{
    public static YdbBytesTypeMapping Default { get; } = new();

    private YdbBytesTypeMapping() : base(
        new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(byte[]),
                jsonValueReaderWriter: JsonByteArrayReaderWriter.Instance
            ),
            storeType: "Bytes",
            dbType: System.Data.DbType.Binary,
            unicode: false
        )
    )
    {
    }

    protected YdbBytesTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new YdbBytesTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var bytes = (byte[])value;
        return $"'{Encoding.UTF8.GetString(bytes)}'";
    }
}
