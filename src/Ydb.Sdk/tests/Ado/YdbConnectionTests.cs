using Microsoft.Extensions.Logging;
using Moq;
using Xunit;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Services.Query;

namespace Ydb.Sdk.Tests.Ado;

[Trait("Category", "Integration")]
public class YdbConnectionTests
{
    [Fact]
    public void ClearPool_WhenHasActiveConnection_CloseActiveConnectionOnClose()
    {
        var connection1 = new YdbConnection();
        var connection2 = new YdbConnection();
        
        connection1.Open();
        connection2.Open();

        YdbConnection.ClosePool(connection1);
        connection1.Close();
        connection2.Close();
    }
}
