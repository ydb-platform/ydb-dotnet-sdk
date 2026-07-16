namespace Ydb.Sdk.Internal;

internal static class MetadataSdkBuildInfoExtensions
{
    private const string SdkBuildInfoToken = "ydb-dotnet-sdk";

    internal static void AddSdkBuildInfo(this Grpc.Core.Metadata metadata)
    {
        var sdkVersion = $"{SdkBuildInfoToken}/{YdbSdkVersion.Value}";
        var clientInfoChain = SdkClientInfoRegistry.Chain;
        var observabilityChain = ObservabilityInfo.BuildChain();

        var sdkBuildInfo = observabilityChain is null ? sdkVersion : $"{sdkVersion};{observabilityChain}";

        metadata.Add(
            Metadata.RpcSdkInfoHeader,
            clientInfoChain is null ? sdkBuildInfo : $"{sdkBuildInfo};{clientInfoChain}");
    }
}
