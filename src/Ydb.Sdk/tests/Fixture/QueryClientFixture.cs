using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Tests.Fixture;

// ReSharper disable once ClassNeverInstantiated.Global
public class QueryClientFixture : DriverFixture
{
    public QueryClient QueryClient { get; }

    public QueryClientFixture()
    {
        QueryClient = new QueryClient(Driver);
    }

    protected override void ClientDispose()
    {
        QueryClient.Dispose();
    }
}
