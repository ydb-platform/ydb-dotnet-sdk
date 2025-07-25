using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Issue;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Utils;

namespace Ydb.Sdk.Ado.Tests.Session;

public class PoolingSessionTests
{
    private const long NodeId = 3;
    private const string SessionId = "sessionId";

    private readonly Mock<IDriver> _mockIDriver;
    private readonly Mock<IServerStream<SessionState>> _mockAttachStream = new(MockBehavior.Strict);
    private readonly PoolingSessionFactory _poolingSessionFactory;
    private readonly PoolingSessionSource<PoolingSession> _poolingSessionSource;

    public PoolingSessionTests()
    {
        var settings = new YdbConnectionStringBuilder();

        _mockIDriver = new Mock<IDriver>(MockBehavior.Strict);
        _mockIDriver.Setup(driver => driver.LoggerFactory).Returns(TestUtils.LoggerFactory);
        _mockIDriver.Setup(driver => driver.ServerStreamCall(
            QueryService.AttachSessionMethod,
            It.Is<AttachSessionRequest>(request => request.SessionId.Equals(SessionId)),
            It.Is<GrpcRequestSettings>(grpcRequestSettings => grpcRequestSettings.NodeId == NodeId))
        ).ReturnsAsync(_mockAttachStream.Object);
        _mockAttachStream.Setup(stream => stream.Dispose());
        _poolingSessionFactory = new PoolingSessionFactory(_mockIDriver.Object, settings, TestUtils.LoggerFactory);
        _poolingSessionSource = new PoolingSessionSource<PoolingSession>(_poolingSessionFactory, settings);
    }

    [Theory]
    [InlineData(StatusCode.Aborted, false)]
    [InlineData(StatusCode.BadSession, true)]
    [InlineData(StatusCode.SessionBusy, true)]
    [InlineData(StatusCode.SessionExpired, true)]
    [InlineData(StatusCode.ClientTransportTimeout, true)]
    [InlineData(StatusCode.ClientTransportUnavailable, true)]
    [InlineData(StatusCode.Overloaded, false)]
    public async Task OnNotSuccessStatusCode_WhenStatusCodeIsNotSuccess_UpdateIsBroken(StatusCode statusCode,
        bool isError)
    {
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        Assert.True(session.IsBroken);
        await session.Open(CancellationToken.None);
        Assert.False(session.IsBroken);
        session.OnNotSuccessStatusCode(statusCode);
        Assert.Equal(isError, session.IsBroken);
        tcsSecondMoveAttachStream.TrySetResult(false);
    }

    [Fact]
    public async Task Open_WhenCreateSessionThrowRpcException_IsBroken()
    {
        _mockIDriver.Setup(driver => driver.UnaryCall(QueryService.CreateSessionMethod,
                It.IsAny<CreateSessionRequest>(),
                It.Is<GrpcRequestSettings>(settings => settings.ClientCapabilities.Contains("session-balancer")))
            )
            .Throws(() => new YdbException(new RpcException(Grpc.Core.Status.DefaultCancelled)));
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Open_WhenCreateSessionReturnBadRequest_IsBroken()
    {
        _mockIDriver.Setup(driver => driver.UnaryCall(QueryService.CreateSessionMethod,
                It.IsAny<CreateSessionRequest>(),
                It.Is<GrpcRequestSettings>(settings => settings.ClientCapabilities.Contains("session-balancer")))
            )
            .ReturnsAsync(
                new CreateSessionResponse
                {
                    Status = StatusIds.Types.StatusCode.BadRequest,
                    Issues = { new IssueMessage { Message = "Mock Issue Message" } }
                }
            );
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        var ydbException = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal("Status: BadRequest, Issues:\n[0] Fatal: Mock Issue Message", ydbException.Message);
        Assert.Equal(StatusCode.BadRequest, ydbException.Code);
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Open_WhenAttachStreamFirstMoveNextAsyncThrowException_IsBroken()
    {
        SetupSuccessCreateSession();
        _mockAttachStream.Setup(attachStream => attachStream.MoveNextAsync(CancellationToken.None))
            .ThrowsAsync(new YdbException(new RpcException(Grpc.Core.Status.DefaultCancelled)));
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        var ydbException = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal("Transport RPC call error", ydbException.Message);
        Assert.Equal(StatusCode.ClientTransportTimeout, ydbException.Code);
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Open_WhenAttachStreamFirstMoveNextIsFalse_IsBroken()
    {
        SetupSuccessCreateSession();
        _mockAttachStream.Setup(attachStream => attachStream.MoveNextAsync(CancellationToken.None)).ReturnsAsync(false);
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        var ydbException = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal("Attach stream is not started!", ydbException.Message);
        Assert.Equal(StatusCode.Cancelled, ydbException.Code);
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Open_WhenAttachStreamFirstCurrentIsBadSession_IsBroken()
    {
        SetupSuccessCreateSession();

        _mockAttachStream.Setup(attachStream => attachStream.MoveNextAsync(CancellationToken.None))
            .ReturnsAsync(true);
        _mockAttachStream.SetupSequence(attachStream => attachStream.Current)
            .Returns(new SessionState
            {
                Status = StatusIds.Types.StatusCode.BadSession,
                Issues = { new IssueMessage { IssueCode = 1, Severity = 1, Message = "Ouch BadSession!" } }
            });
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        var ydbException = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal("Status: BadSession, Issues:\n[1] Error: Ouch BadSession!", ydbException.Message);
        Assert.Equal(StatusCode.BadSession, ydbException.Code);
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Open_WhenSuccessOpenThenAttachStreamIsClosed_IsBroken()
    {
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await session.Open(CancellationToken.None);
        Assert.False(session.IsBroken);
        tcsSecondMoveAttachStream.TrySetResult(false); // attach stream is closed
        await Task.Delay(500);
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Open_WhenSuccessOpenThenAttachStreamSendRpcException_IsNotBroken()
    {
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await session.Open(CancellationToken.None);
        Assert.False(session.IsBroken);
        tcsSecondMoveAttachStream.SetException(
            new YdbException(new RpcException(Grpc.Core.Status.DefaultCancelled))); // attach stream is closed
        await Task.Delay(500);
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Open_WhenSuccessOpenThenAttachStreamSendBadSession_IsNotBroken()
    {
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await session.Open(CancellationToken.None);
        Assert.False(session.IsBroken);
        tcsSecondMoveAttachStream.SetResult(true); // attach stream is closed
        await Task.Delay(500);
        Assert.True(session.IsBroken);
    }

    [Fact]
    public async Task Delete_WhenSuccessOpenSession_IsBroken()
    {
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await session.Open(CancellationToken.None);
        _mockIDriver.Setup(driver => driver.UnaryCall(
            QueryService.DeleteSessionMethod,
            It.Is<DeleteSessionRequest>(request => request.SessionId.Equals(SessionId)),
            It.Is<GrpcRequestSettings>(grpcRequestSettings => grpcRequestSettings.NodeId == NodeId))
        );
        Assert.False(session.IsBroken);
        await session.DeleteSession();
        Assert.True(session.IsBroken);
        _mockIDriver.Verify(driver => driver.UnaryCall(QueryService.DeleteSessionMethod,
            It.IsAny<DeleteSessionRequest>(), It.IsAny<GrpcRequestSettings>()));
        tcsSecondMoveAttachStream.TrySetResult(false);
    }

    [Fact]
    public async Task Delete_WhenSuccessOpenSessionBadSessionInAttachStream_IsBroken()
    {
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await session.Open(CancellationToken.None);
        _mockIDriver.Setup(driver => driver.UnaryCall(
            QueryService.DeleteSessionMethod,
            It.Is<DeleteSessionRequest>(request => request.SessionId.Equals(SessionId)),
            It.Is<GrpcRequestSettings>(grpcRequestSettings => grpcRequestSettings.NodeId == NodeId))
        );
        Assert.False(session.IsBroken);
        session.OnNotSuccessStatusCode(StatusCode.BadSession);
        await session.DeleteSession();
        Assert.True(session.IsBroken);
        _mockIDriver.Verify(driver => driver.UnaryCall(QueryService.DeleteSessionMethod,
            It.IsAny<DeleteSessionRequest>(), It.IsAny<GrpcRequestSettings>()), Times.Never);
        tcsSecondMoveAttachStream.TrySetResult(false);
    }

    [Fact]
    public async Task CommitTransaction_WhenSuccessOpenSession_AssertNodeId_SessionId_YdbException()
    {
        const string txId = "txId";
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await session.Open(CancellationToken.None);
        _mockIDriver.Setup(driver => driver.UnaryCall(
            QueryService.CommitTransactionMethod,
            It.Is<CommitTransactionRequest>(request =>
                request.SessionId.Equals(SessionId) && request.TxId.Equals(txId)),
            It.Is<GrpcRequestSettings>(grpcRequestSettings => grpcRequestSettings.NodeId == NodeId))
        ).ThrowsAsync(YdbException.FromServer(StatusIds.Types.StatusCode.Aborted, []));
        Assert.False(session.IsBroken);
        var ydbException = await Assert.ThrowsAsync<YdbException>(() => session.CommitTransaction(txId));
        Assert.Equal(StatusCode.Aborted, ydbException.Code);
        Assert.Equal("Status: Aborted", ydbException.Message);
        tcsSecondMoveAttachStream.TrySetResult(false);
    }

    [Fact]
    public async Task RollbackTransaction_WhenSuccessOpenSession_AssertNodeId_SessionId_YdbException()
    {
        const string txId = "txId";
        SetupSuccessCreateSession();
        var tcsSecondMoveAttachStream = SetupAttachStream();
        var session = _poolingSessionFactory.NewSession(_poolingSessionSource);
        await session.Open(CancellationToken.None);
        _mockIDriver.Setup(driver => driver.UnaryCall(
            QueryService.RollbackTransactionMethod,
            It.Is<RollbackTransactionRequest>(request =>
                request.SessionId.Equals(SessionId) && request.TxId.Equals(txId)),
            It.Is<GrpcRequestSettings>(grpcRequestSettings => grpcRequestSettings.NodeId == NodeId))
        ).ThrowsAsync(YdbException.FromServer(StatusIds.Types.StatusCode.NotFound, []));
        Assert.False(session.IsBroken);
        var ydbException = await Assert.ThrowsAsync<YdbException>(() => session.RollbackTransaction(txId));
        Assert.Equal(StatusCode.NotFound, ydbException.Code);
        Assert.Equal("Status: NotFound", ydbException.Message);
        tcsSecondMoveAttachStream.TrySetResult(false);
    }

    private TaskCompletionSource<bool> SetupAttachStream()
    {
        var tcsSecondMoveAttachStream = new TaskCompletionSource<bool>();

        _mockAttachStream.SetupSequence(attachStream => attachStream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(new ValueTask<bool>(tcsSecondMoveAttachStream.Task));
        _mockAttachStream.SetupSequence(attachStream => attachStream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success })
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.BadSession });
        return tcsSecondMoveAttachStream;
    }

    private void SetupSuccessCreateSession() => _mockIDriver
        .Setup(driver => driver.UnaryCall(QueryService.CreateSessionMethod,
            It.IsAny<CreateSessionRequest>(),
            It.Is<GrpcRequestSettings>(settings => settings.ClientCapabilities.Contains("session-balancer")))
        )
        .ReturnsAsync(
            new CreateSessionResponse
            {
                Status = StatusIds.Types.StatusCode.Success,
                SessionId = SessionId,
                NodeId = NodeId
            }
        );
}
