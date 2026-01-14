using System.Reflection;
using Microsoft.Extensions.Logging;

namespace Ydb.Sdk.Auth;

internal static class CredentialsProviderUtils
{
    private const string AssemblyYcName = "Ydb.Sdk.Yc.Auth";
    private const string ServiceAccountProviderTypeName = "Ydb.Sdk.Yc.ServiceAccountProvider";
    private const string MetadataProviderTypeName = "Ydb.Sdk.Yc.MetadataProvider";

    internal static ICredentialsProvider LoadServiceAccountProvider(
        string serviceAccountKeyFilePath,
        ILoggerFactory loggerFactory
    )
    {
        try
        {
            return (ICredentialsProvider)Assembly.Load(AssemblyYcName)
                .GetType(ServiceAccountProviderTypeName, throwOnError: true)!
                .GetConstructor(new[] { typeof(string), typeof(ILoggerFactory) })!
                .Invoke(new object[] { serviceAccountKeyFilePath, loggerFactory });
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Could not load Ydb.Sdk.Yc.ServiceAccountProvider", e);
        }
    }

    internal static ICredentialsProvider LoadMetadataProvider(ILoggerFactory loggerFactory)
    {
        try
        {
            return (ICredentialsProvider)Assembly.Load(AssemblyYcName)
                .GetType(MetadataProviderTypeName, throwOnError: true)!
                .GetConstructor(new[] { typeof(ILoggerFactory) })!
                .Invoke(new object[] { loggerFactory });
        }
        catch (Exception e)
        {
            throw new InvalidOperationException("Could not load Ydb.Sdk.MetadataProvider", e);
        }
    }
}
