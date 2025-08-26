namespace Ydb.Sdk.Ado.Internal;

internal static class TypeExtensions
{
    internal static bool IsNull(this Type type) => type.TypeCase == Type.TypeOneofCase.NullType;
    
    internal static bool IsOptional(this Type type) => type.TypeCase == Type.TypeOneofCase.OptionalType;
}
