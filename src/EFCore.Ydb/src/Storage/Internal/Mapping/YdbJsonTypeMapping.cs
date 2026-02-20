using System;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.Json;
using Microsoft.EntityFrameworkCore.Storage.Json;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Storage.Internal.Mapping;

public class YdbJsonTypeMapping : YdbTypeMapping
{
    public YdbJsonTypeMapping(Type clrType, YdbDbType ydbDbType) :
        base(clrType, ydbDbType, JsonStringReaderWriter.Instance)
    {
    }

    protected YdbJsonTypeMapping(RelationalTypeMappingParameters parameters, YdbDbType ydbDbType) :
        base(parameters, ydbDbType)
    {
    }

    protected override YdbJsonTypeMapping Clone(RelationalTypeMappingParameters parameters) =>
        new(parameters, YdbDbType);

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
                return $"{YdbDbType}('{Encoding.UTF8.GetString(stream.ToArray())}')";
            }
            case string s:
                return $"{YdbDbType}('{s}')";
            default:
                return $"{YdbDbType}('{JsonSerializer.Serialize(value)}')";
        }
    }

    public override Expression GenerateCodeLiteral(object value)
        => value switch
        {
            JsonDocument document => Expression.Call(
                ParseMethod, Expression.Constant(document.RootElement.ToString()), DefaultJsonDocumentOptions),
            JsonElement element => Expression.Property(
                Expression.Call(ParseMethod, Expression.Constant(element.ToString()), DefaultJsonDocumentOptions),
                nameof(JsonDocument.RootElement)),
            string s => Expression.Constant(s),
            _ => throw new NotSupportedException("Cannot generate code literals for JSON POCOs")
        };

    private static readonly Expression DefaultJsonDocumentOptions = Expression.New(typeof(JsonDocumentOptions));

    private static readonly MethodInfo ParseMethod =
        typeof(JsonDocument).GetMethod(nameof(JsonDocument.Parse), [typeof(string), typeof(JsonDocumentOptions)])!;
}
