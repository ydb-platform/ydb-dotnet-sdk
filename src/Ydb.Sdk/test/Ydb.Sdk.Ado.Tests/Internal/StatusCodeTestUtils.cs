using Xunit;
using Ydb.Sdk.Ado.Internal;

namespace Ydb.Sdk.Ado.Tests.Internal;

public class StatusCodeTestUtils
{
    [Theory]
    [InlineData(Grpc.Core.StatusCode.Unavailable, StatusCode.ClientTransportUnavailable)]
    [InlineData(Grpc.Core.StatusCode.DeadlineExceeded, StatusCode.ClientTransportTimeout)]
    [InlineData(Grpc.Core.StatusCode.ResourceExhausted, StatusCode.ClientTransportResourceExhausted)]
    [InlineData(Grpc.Core.StatusCode.Unimplemented, StatusCode.ClientTransportUnimplemented)]
    [InlineData(Grpc.Core.StatusCode.Cancelled, StatusCode.Cancelled)]
    public void Code_GrpcCoreStatusCodeConvertToStatusCode_Assert(
        Grpc.Core.StatusCode statusCode,
        StatusCode expectedStatusCode
    ) => Assert.Equal(expectedStatusCode, new Grpc.Core.Status(statusCode, "Mock status").Code());
    
    
}
