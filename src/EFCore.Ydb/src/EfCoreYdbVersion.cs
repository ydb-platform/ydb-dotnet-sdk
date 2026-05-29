using System.Reflection;

namespace EntityFrameworkCore.Ydb;

internal static class EfCoreYdbVersion
{
    internal static readonly string Value = typeof(EfCoreYdbVersion).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
        .InformationalVersion ?? "UNKNOWN";
}
