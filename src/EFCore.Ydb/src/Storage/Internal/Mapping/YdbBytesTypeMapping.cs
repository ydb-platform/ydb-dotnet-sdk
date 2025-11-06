using System.Text;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbBytesTypeMapping : RelationalTypeMapping
{
    public static YdbBytesTypeMapping Default { get; } = new();

    private YdbBytesTypeMapping() : base("Bytes", typeof(byte[]), System.Data.DbType.Binary,
        jsonValueReaderWriter: JsonByteArrayReaderWriter.Instance)
    {
    }

    protected YdbBytesTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    protected override YdbBytesTypeMapping Clone(RelationalTypeMappingParameters parameters) => new(parameters);

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        var bytes = (byte[])value;
        return $"'{Encoding.UTF8.GetString(bytes)}'";
    }
}
