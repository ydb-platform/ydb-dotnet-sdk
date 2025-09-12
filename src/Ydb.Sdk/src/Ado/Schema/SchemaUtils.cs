namespace Ydb.Sdk.Ado.Schema;

internal static class SchemaUtils
{
    internal static string YqlTableType(this Type type) => type.TypeCase switch
    {
        Type.TypeOneofCase.OptionalType => type.OptionalType.Item.YqlTableType(),
        Type.TypeOneofCase.TypeId =>
            type.TypeId switch
            {
                Type.Types.PrimitiveTypeId.Utf8 => "Text",
                Type.Types.PrimitiveTypeId.String => "Bytes",
                _ => type.TypeId.ToString()
            },
        Type.TypeOneofCase.DecimalType => $"Decimal({type.DecimalType.Precision}, {type.DecimalType.Scale})",
        _ => "Unknown"
    };

    internal static bool IsSystem(this string path) => path.StartsWith(".sys/")
                                                       || path.StartsWith(".sys_health/")
                                                       || path.StartsWith(".sys_health_dev/");
}
