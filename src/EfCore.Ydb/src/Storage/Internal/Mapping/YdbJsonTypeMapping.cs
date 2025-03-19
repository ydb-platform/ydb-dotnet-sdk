using System;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage;

namespace EfCore.Ydb.Storage.Internal.Mapping;

public class YdbJsonTypeMapping : JsonTypeMapping
{
    public YdbJsonTypeMapping(string storeType, Type clrType, DbType? dbType) : base(storeType, clrType, dbType)
    {
    }

    protected YdbJsonTypeMapping(RelationalTypeMappingParameters parameters) : base(parameters)
    {
    }

    private static readonly MethodInfo GetStringMethod
        = typeof(DbDataReader).GetRuntimeMethod(nameof(DbDataReader.GetString), [typeof(int)])!;

    private static readonly PropertyInfo Utf8Property
        = typeof(Encoding).GetProperty(nameof(Encoding.UTF8))!;

    private static readonly MethodInfo EncodingGetBytesMethod
        = typeof(Encoding).GetMethod(nameof(Encoding.GetBytes), [typeof(string)])!;

    private static readonly ConstructorInfo MemoryStreamConstructor
        = typeof(MemoryStream).GetConstructor([typeof(byte[])])!;

    protected override RelationalTypeMapping Clone(RelationalTypeMappingParameters parameters)
        => new YdbJsonTypeMapping(parameters);

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
        MemoryStreamConstructor,
        Expression.Call(
            Expression.Property(null, Utf8Property),
            EncodingGetBytesMethod,
            expression)
    );
}
