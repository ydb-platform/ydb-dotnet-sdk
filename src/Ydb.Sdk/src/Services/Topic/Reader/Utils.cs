namespace Ydb.Sdk.Services.Topic.Reader;

internal static class Utils
{
    internal static long CalculateApproximatelyBytesSize(long bytesSize, int countParts, int currentIndex)
    {
        return bytesSize / countParts + (currentIndex == countParts - 1 ? bytesSize % countParts : 0);
    }
}
