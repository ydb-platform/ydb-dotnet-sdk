namespace Ydb.Sdk.Ado.Schema;

internal static class SchemaUtils
{
    internal static string YqlTableType(this Type type)
    {
        var typeId = type.TypeCase == Type.TypeOneofCase.OptionalType
            ? type.OptionalType.Item.TypeId
            : type.TypeId;

        return typeId switch
        {
            Type.Types.PrimitiveTypeId.Utf8 => "Text",
            Type.Types.PrimitiveTypeId.String => "Bytes",
            _ => typeId.ToString()
        };
    }

    internal static bool IsSystem(this string path) => path.StartsWith(".sys/")
                                                       || path.StartsWith(".sys_health/")
                                                       || path.StartsWith(".sys_health_dev/");
}
