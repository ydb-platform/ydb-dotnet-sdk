using Ydb.Sdk.Services.Table;

namespace Ydb.Sdk.Tests.Fixture;

// ReSharper disable once ClassNeverInstantiated.Global
public class TableClientFixture : DriverFixture
{
    public TableClient TableClient { get; }

    public TableClientFixture()
    {
        TableClient = new TableClient(Driver);
    }

    protected override void ClientDispose() => TableClient.Dispose();
}
