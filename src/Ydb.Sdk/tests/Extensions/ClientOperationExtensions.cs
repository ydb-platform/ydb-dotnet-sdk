using Xunit;
using Ydb.Sdk.Client;

namespace Ydb.Sdk.Tests.Extensions;

public static class ClientOperationExtensions
{
    public static void AssertIsSuccess(this ClientOperation operation)
        => Assert.True(operation.Status.IsSuccess, operation.Status.ToString());
}
