using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Microsoft.EntityFrameworkCore.Storage.ValueConversion;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbStringTypeMapping : RelationalTypeMapping
{
    public static YdbStringTypeMapping Default { get; } = new();

    private YdbStringTypeMapping() : base(
        new RelationalTypeMappingParameters(
            new CoreTypeMappingParameters(
                typeof(byte[]),
                new StringToBytesConverter(Encoding.UTF8),
                jsonValueReaderWriter: JsonByteArrayReaderWriter.Instance
            ),
            storeType: "String",
            storeTypePostfix: StoreTypePostfix.None,
            dbType: System.Data.DbType.Binary,
            unicode: false
        )
    )
    {
    }

    protected YdbStringTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new YdbStringTypeMapping(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var bytes = (byte[])value;
        return $"'{Encoding.UTF8.GetString(bytes)}'";
    }
}
