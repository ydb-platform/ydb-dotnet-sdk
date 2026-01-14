using Xunit;

namespace Ydb.Sdk.Ado.Tests;

public class YcCredentialsReflectionLoaderTests
{
    [Fact]
    public async Task YcAssemblyNotResolved_ServiceAccountProvider_Throws_InvalidOperationException()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new YdbConnection("ServiceAccountKeyFilePath=/path/to/key.json").OpenAsync());

        Assert.StartsWith("Could not load Ydb.Sdk.Yc.ServiceAccountProvider", ex.Message);
        Assert.NotNull(ex.InnerException);
        Assert.IsType<FileNotFoundException>(ex.InnerException);
    }

    [Fact]
    public async Task YcAssemblyNotResolved_MetadataProvider_Throws_InvalidOperationException()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            new YdbConnection("EnableMetadataCredentials=true").OpenAsync());

        Assert.StartsWith("Could not load Ydb.Sdk.MetadataProvider", ex.Message);
        Assert.NotNull(ex.InnerException);
        Assert.IsType<FileNotFoundException>(ex.InnerException);
    }
}
