using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbJsonTypeMapping : JsonTypeMapping, IYdbTypeMapping
{
    public YdbJsonTypeMapping(string storeType, Type clrType, DbType? dbType) : base(storeType, clrType, dbType)
    {
    }

    protected YdbJsonTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    private static readonly MethodInfo GetStringMethod
        = typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetString), [typeof(int)]) ?? throw new Exception();

    private static readonly PropertyInfo? Utf8Property
        = typeof(Encoding).GetProperty(nameof(Encoding.UTF8));

    private static readonly MethodInfo? EncodingGetBytesMethod
        = typeof(Encoding).GetMethod(nameof(Encoding.GetBytes), [typeof(string)]);

    private static readonly ConstructorInfo? MemoryStreamConstructor
        = typeof(MemoryStream).GetConstructor([typeof(byte[])]);

    protected override YdbJsonTypeMapping Clone(RelationalTypeMappingParameters parameters) => new(parameters);

    public override MethodInfo GetDataReaderMethod() => GetStringMethod;

    protected override string GenerateNonNullSqlLiteral(object value)
    {
        switch (value)
        {
            case JsonDocument:
            case JsonElement:
            {
                using var stream = new MemoryStream();
                using var writer = new Utf8JsonWriter(stream);
                if (value is JsonDocument doc)
                {
                    doc.WriteTo(writer);
                }
                else
                {
                    ((JsonElement)value).WriteTo(writer);
                }

                writer.Flush();
                return $"'{Encoding.UTF8.GetString(stream.ToArray())}'";
            }
            case string s:
                return $"'{s}'";
            default:
                return $"'{JsonSerializer.Serialize(value)}'";
        }
    }

    public override Expression CustomizeDataReaderExpression(Expression expression) => Expression.New(
        MemoryStreamConstructor ?? throw new Exception(),
        Expression.Call(
            Expression.Property(null, Utf8Property ?? throw new Exception()),
            EncodingGetBytesMethod ?? throw new Exception(),
            expression)
    );

    public YdbDbType YdbDbType => YdbDbType.Json;

    protected override void ConfigureParameter(DbParameter parameter) =>
        ((YdbParameter)parameter).YdbDbType = YdbDbType;
}
