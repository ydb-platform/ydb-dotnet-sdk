using System.Reflection;

namespace Ydb.Sdk.Internal;

internal static class YdbSdkVersion
{
    internal static readonly string Value = typeof(YdbSdkVersion).Assembly
        .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
        .InformationalVersion ?? "unknown";
}
