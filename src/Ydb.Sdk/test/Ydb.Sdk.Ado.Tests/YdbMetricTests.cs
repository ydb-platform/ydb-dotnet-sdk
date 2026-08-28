using System.Data;
using System.Diagnostics;
using Grpc.Core;
using Moq;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Xunit;
using Ydb.Query;
using Ydb.Query.V1;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Ado.Session;
using Ydb.Sdk.Ado.Tests.Utils;
using Ydb.Sdk.OpenTelemetry;

namespace Ydb.Sdk.Ado.Tests;

[Collection("DisableParallelization")]
public class YdbMetricTests : TestBase
{
    private const string MockSessionId = "sessionId";
    private const long MockNodeId = 3;

    private static readonly YdbConnectionStringBuilder BaseConnectionSettings = new(TestUtils.ConnectionString)
    {
        PoolName = "ado-metrics-tests"
    };

    private static string EndpointFor(YdbConnectionStringBuilder settings) => $"{settings.Host}:{settings.Port}";

    [Fact]
    public async Task OperationDuration()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);
        await using var conn = await dataSource.OpenConnectionAsync();

        await new YdbCommand("SELECT 1;", conn).ExecuteNonQueryAsync();

        await using var txConn = await dataSource.OpenConnectionAsync();
        await using var tx = await txConn.BeginTransactionAsync();
        await new YdbCommand("SELECT 1;", txConn).ExecuteNonQueryAsync();
        await tx.CommitAsync();

        await using var rollbackConn = await dataSource.OpenConnectionAsync();
        await using var rollbackTx = await rollbackConn.BeginTransactionAsync();
        await rollbackTx.RollbackAsync();

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.client.operation.duration");
        Assert.NotNull(metric);

        var points = GetFilteredPoints(metric.GetMetricPoints())
            .ToDictionary(p => (string)ToDictionary(p.Tags)["operation.name"]!);

        Assert.True(points["ExecuteQuery"].GetHistogramSum() > 0);
        Assert.True(points["Commit"].GetHistogramSum() > 0);
        Assert.True(points["Rollback"].GetHistogramSum() > 0);

        var tags = ToDictionary(points["ExecuteQuery"].Tags);
        Assert.Equal(settings.Database, tags["database"]);
        Assert.Equal(EndpointFor(settings), tags["endpoint"]);
        Assert.Equal("ExecuteQuery", tags["operation.name"]);
    }

    [Fact]
    public async Task ConnectionCount()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);

        await using (var _ = await dataSource.OpenConnectionAsync())
        {
            meterProvider.ForceFlush();

            var metric = GetMetric(exportedItems, "ydb.query.session.count");
            var points = GetConnectionCountPoints(metric.GetMetricPoints(), settings.PoolName!).ToList();

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(1, usedPoint.GetGaugeLastValueLong());

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(0, idlePoint.GetGaugeLastValueLong());

            exportedItems.Clear();
        }

        meterProvider.ForceFlush();

        {
            var metric = GetMetric(exportedItems, "ydb.query.session.count");
            var points = GetConnectionCountPoints(metric.GetMetricPoints(), settings.PoolName!).ToList();

            var usedPoint = GetPoint(points, "used");
            Assert.Equal(0, usedPoint.GetGaugeLastValueLong());

            var idlePoint = GetPoint(points, "idle");
            Assert.Equal(1, idlePoint.GetGaugeLastValueLong());
        }
    }

    [Fact]
    public async Task OperationFailed()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);
        await using var conn = await dataSource.OpenConnectionAsync();

        await Assert.ThrowsAnyAsync<Exception>(async () =>
            await new YdbCommand("SELECT * FROM table_that_does_not_exist_xyz", conn).ExecuteScalarAsync());

        meterProvider.ForceFlush();

        var failed = GetMetric(exportedItems, "ydb.client.operation.failed");
        Assert.NotNull(failed);
        var point = GetFilteredPoints(failed.GetMetricPoints()).Single();
        Assert.Equal(1, point.GetSumLong());

        var tags = ToDictionary(point.Tags);
        Assert.Equal(settings.Database, tags["database"]);
        Assert.Equal(EndpointFor(settings), tags["endpoint"]);
        Assert.Equal("ExecuteQuery", tags["operation.name"]);
        Assert.NotNull(tags["status_code"]);
    }

    [Fact]
    public async Task OperationFailed_CreateSessionUnaryCall()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder => builder.PoolName = "ado-metrics-create-session-unary");
        var driver = CreateMockDriver();
        driver.Setup(d => d.UnaryCall(
                QueryService.CreateSessionMethod,
                It.IsAny<CreateSessionRequest>(),
                It.Is<GrpcRequestSettings>(s => s.ClientCapabilities.Contains("session-balancer"))))
            .ThrowsAsync(new YdbException(
                new RpcException(new Status(Grpc.Core.StatusCode.ResourceExhausted, "Mock exhausted"))));

        var factory = new PoolingSessionFactory(driver.Object, settings);
        await using var source = new PoolingSessionSource<PoolingSession>(factory, settings);
        var session = factory.NewSession(source);

        var ex = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal(StatusCode.ClientTransportResourceExhausted, ex.Code);

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.client.operation.failed");
        var point = GetOperationFailedPoint(
            metric.GetMetricPoints(),
            settings,
            operationName: "CreateSession",
            statusCode: "ClientTransportResourceExhausted");

        Assert.Equal(1, point.GetSumLong());
    }

    [Fact]
    public async Task OperationFailed_CreateSessionAttachStream()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder => builder.PoolName = "ado-metrics-create-session-attach");
        var driver = CreateMockDriver();
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);

        driver.Setup(d => d.UnaryCall(
                QueryService.CreateSessionMethod,
                It.IsAny<CreateSessionRequest>(),
                It.Is<GrpcRequestSettings>(s => s.ClientCapabilities.Contains("session-balancer"))))
            .ReturnsAsync(new CreateSessionResponse
            {
                Status = StatusIds.Types.StatusCode.Success,
                SessionId = "sessionId",
                NodeId = 3
            });

        driver.Setup(d => d.ServerStreamCall(
                QueryService.AttachSessionMethod,
                It.Is<AttachSessionRequest>(r => r.SessionId == "sessionId"),
                It.Is<GrpcRequestSettings>(s => s.NodeId == 3)))
            .ReturnsAsync(attachStream.Object);

        attachStream.Setup(s => s.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new YdbException(
                new RpcException(new Status(Grpc.Core.StatusCode.ResourceExhausted, "Mock exhausted"))));
        attachStream.Setup(s => s.Dispose());

        var factory = new PoolingSessionFactory(driver.Object, settings);
        await using var source = new PoolingSessionSource<PoolingSession>(factory, settings);
        var session = factory.NewSession(source);

        var ex = await Assert.ThrowsAsync<YdbException>(() => session.Open(CancellationToken.None));
        Assert.Equal(StatusCode.ClientTransportResourceExhausted, ex.Code);

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.client.operation.failed");
        var point = GetOperationFailedPoint(
            metric.GetMetricPoints(),
            settings,
            operationName: "CreateSession",
            statusCode: "ClientTransportResourceExhausted");

        Assert.Equal(1, point.GetSumLong());
    }

    [Fact]
    public async Task ConnectionCreateTime()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings();
        await using var dataSource = new YdbDataSource(settings);
        await using var _ = await dataSource.OpenConnectionAsync();

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.query.session.create_time");
        var point = GetPoolPoints(metric.GetMetricPoints(), settings.PoolName!).Single();

        Assert.True(point.GetHistogramSum() > 0);
        Assert.Equal(settings.PoolName, ToDictionary(point.Tags)["ydb.query.session.pool.name"]);
    }

    [Theory]
    [InlineData(SessionState.SessionHintOneofCase.NodeShutdown, "node_shutdown")]
    [InlineData(SessionState.SessionHintOneofCase.SessionShutdown, "session_shutdown")]
    public async Task SessionClosed_WhenAttachStreamSendsShutdownHint_ReportsReason(
        SessionState.SessionHintOneofCase hint,
        string reason)
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.PoolName = $"ado-metrics-session-closed-{reason}";
        });
        var lifecycleState = new SessionState { Status = StatusIds.Types.StatusCode.Success };
        if (hint == SessionState.SessionHintOneofCase.NodeShutdown)
            lifecycleState.NodeShutdown = new NodeShutdownHint();
        else
            lifecycleState.SessionShutdown = new SessionShutdownHint();

        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        attachStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(true);
        attachStream.SetupSequence(stream => stream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success })
            .Returns(lifecycleState);
        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var driver = CreatePoolingDriver(attachStream.Object);

        await using var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);
        var session = await source.OpenSession();
        await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(session.IsBroken);
        session.Dispose();

        meterProvider.ForceFlush();
        AssertSessionClosed(exportedItems, settings, reason);
    }

    [Theory]
    [InlineData(StatusCode.BadSession, "bad_session")]
    [InlineData(StatusCode.SessionExpired, "bad_session")]
    [InlineData(StatusCode.SessionBusy, "session_busy")]
    [InlineData(StatusCode.ClientTransportTimeout, "client_timeout")]
    [InlineData(StatusCode.ClientCancelled, "client_cancelled")]
    [InlineData(StatusCode.ClientTransportUnavailable, "transport_error")]
    [InlineData(StatusCode.ClientTransportResourceExhausted, "transport_error")]
    [InlineData(StatusCode.ClientTransportUnknown, "transport_error")]
    public async Task SessionClosed_WhenStatusRetiresSession_ReportsReason(StatusCode statusCode, string reason)
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.PoolName = $"ado-metrics-session-status-{statusCode}";
        });
        var lifecycleAttach = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        attachStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(lifecycleAttach.Task);
        attachStream.Setup(stream => stream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success });
        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var driver = CreatePoolingDriver(attachStream.Object);
        await using var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);
        var session = await source.OpenSession();

        session.OnNotSuccessStatusCode(statusCode);
        Assert.True(session.IsBroken);

        lifecycleAttach.SetResult(false);
        await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        session.Dispose();

        meterProvider.ForceFlush();
        AssertSessionClosed(exportedItems, settings, reason);
    }

    [Theory]
    [InlineData(false, "eof")]
    [InlineData(false, "hint")]
    [InlineData(false, "error")]
    [InlineData(true, "eof")]
    [InlineData(true, "hint")]
    [InlineData(true, "error")]
    public async Task SessionClosed_SessionBusyBeforeLateAttachTermination_ReportsOnce(
        bool retryable,
        string lateAttachTermination)
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.PoolName = $"ado-metrics-query-error-{retryable}-{lateAttachTermination}";
        });
        var lifecycleAttach = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        attachStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(lifecycleAttach.Task);
        attachStream.SetupSequence(stream => stream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success })
            .Returns(new SessionState
            {
                Status = StatusIds.Types.StatusCode.Success,
                SessionShutdown = new SessionShutdownHint()
            });
        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var queryErrorObserved = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var queryDrain = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var queryStream = new Mock<IServerStream<ExecuteQueryResponsePart>>(MockBehavior.Strict);
        queryStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(queryDrain.Task);
        queryStream.Setup(stream => stream.Current)
            .Callback(() => queryErrorObserved.TrySetResult())
            .Returns(new ExecuteQueryResponsePart { Status = StatusIds.Types.StatusCode.SessionBusy });

        var driver = CreatePoolingDriver(attachStream.Object, queryStream.Object);

        await using var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);
        Assert.True(PoolManager.Pools.TryAdd(settings.PoolKey, source));
        try
        {
            await using var connection = new YdbConnection(settings);
            if (retryable)
                await connection.OpenAsync(new YdbRetryPolicyExecutor(
                    new YdbRetryPolicy(new YdbRetryPolicyConfig { MaxAttempts = 1 })));
            else
                await connection.OpenAsync();

            var queryTask = new YdbCommand("SELECT 1", connection).ExecuteReaderAsync();
            await queryErrorObserved.Task.WaitAsync(TimeSpan.FromSeconds(5));

            queryDrain.SetResult(false);
            var exception = await Assert.ThrowsAsync<YdbException>(() => queryTask);
            Assert.Equal(StatusCode.SessionBusy, exception.Code);
            Assert.Equal(retryable ? ConnectionState.Open : ConnectionState.Broken, connection.State);

            switch (lateAttachTermination)
            {
                case "eof":
                    lifecycleAttach.SetResult(false);
                    break;
                case "hint":
                    lifecycleAttach.SetResult(true);
                    break;
                case "error":
                    lifecycleAttach.SetException(
                        new YdbException(StatusCode.ClientTransportUnavailable, "late attach error"));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(lateAttachTermination));
            }

            await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));

            await connection.CloseAsync();
            meterProvider.ForceFlush();

            AssertSessionClosed(exportedItems, settings, "session_busy");
            driver.Verify(d => d.UnaryCall(
                QueryService.DeleteSessionMethod,
                It.IsAny<DeleteSessionRequest>(),
                It.IsAny<GrpcRequestSettings>()), Times.AtMostOnce());
        }
        finally
        {
            Assert.True(PoolManager.Pools.TryRemove(settings.PoolKey, out _));
        }
    }

    [Theory]
    [InlineData(false, "attach_closed")]
    [InlineData(true, "transport_error")]
    public async Task SessionClosed_WhenActiveAttachStreamTerminates_ReportsReason(
        bool transportError,
        string reason)
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.PoolName = $"ado-metrics-{reason}";
        });
        var lifecycleAttach = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        attachStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(lifecycleAttach.Task);
        attachStream.Setup(stream => stream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success });
        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var driver = CreatePoolingDriver(attachStream.Object);
        await using var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);
        var session = await source.OpenSession();

        if (transportError)
        {
            lifecycleAttach.SetException(
                new YdbException(StatusCode.ClientTransportUnavailable, "attach transport error"));
        }
        else
        {
            lifecycleAttach.SetResult(false);
        }

        await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.True(session.IsBroken);
        session.Dispose();

        meterProvider.ForceFlush();
        AssertSessionClosed(exportedItems, settings, reason);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SessionClosed_WhenPrimaryAttachFails_DoesNotReport(bool transportError)
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
            builder.PoolName = $"ado-metrics-primary-attach-{transportError}");
        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        var firstMove = attachStream.Setup(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()));
        if (transportError)
        {
            firstMove.ThrowsAsync(
                new YdbException(StatusCode.ClientTransportUnavailable, "primary attach transport error"));
        }
        else
        {
            firstMove.ReturnsAsync(false);
        }

        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var driver = CreatePoolingDriver(attachStream.Object);
        await using var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);

        await Assert.ThrowsAsync<YdbException>(() => source.OpenSession().AsTask());
        await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));

        meterProvider.ForceFlush();
        AssertSessionNotClosed(exportedItems, settings.PoolName!);
    }

    [Theory]
    [InlineData(Grpc.Core.StatusCode.DeadlineExceeded)]
    [InlineData(Grpc.Core.StatusCode.Cancelled)]
    public async Task SessionClosed_QueryTransportTimeout_ReportsReason(Grpc.Core.StatusCode grpcStatusCode)
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.PoolName = $"ado-metrics-query-timeout-{grpcStatusCode}";
        });
        var lifecycleAttach = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        attachStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(lifecycleAttach.Task);
        attachStream.Setup(stream => stream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success });
        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var queryStream = new Mock<IServerStream<ExecuteQueryResponsePart>>(MockBehavior.Strict);
        queryStream.Setup(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new YdbException(new RpcException(new Status(grpcStatusCode, "query transport timeout"))));

        var driver = CreatePoolingDriver(attachStream.Object, queryStream.Object);
        await using var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);
        Assert.True(PoolManager.Pools.TryAdd(settings.PoolKey, source));
        try
        {
            await using var connection = new YdbConnection(settings);
            await connection.OpenAsync();

            var exception = await Assert.ThrowsAsync<YdbException>(() =>
                new YdbCommand("SELECT 1", connection).ExecuteReaderAsync());
            Assert.Equal(StatusCode.ClientTransportTimeout, exception.Code);

            Assert.Equal(ConnectionState.Broken, connection.State);

            lifecycleAttach.SetResult(false);
            await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await connection.CloseAsync();

            meterProvider.ForceFlush();
            AssertSessionClosed(exportedItems, settings, "client_timeout");
        }
        finally
        {
            Assert.True(PoolManager.Pools.TryRemove(settings.PoolKey, out _));
        }
    }

    [Fact]
    public async Task SessionClosed_WhenClientClosesUnreadQueryStream_ReportsReason()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.PoolName = "ado-metrics-query-stream-cancelled";
        });
        var lifecycleAttach = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        attachStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(lifecycleAttach.Task);
        attachStream.Setup(stream => stream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success });
        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var resultSet = ResultSet.Parser.ParseJson(
            "{ \"columns\": [ { \"name\": \"column0\", \"type\": { \"typeId\": \"BOOL\" } } ], " +
            "\"rows\": [ { \"items\": [ { \"boolValue\": true } ] } ] }");
        var queryStream = new Mock<IServerStream<ExecuteQueryResponsePart>>(MockBehavior.Strict);
        queryStream.Setup(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>())).ReturnsAsync(true);
        queryStream.Setup(stream => stream.Current).Returns(new ExecuteQueryResponsePart
        {
            Status = StatusIds.Types.StatusCode.Success,
            ResultSet = resultSet
        });
        queryStream.Setup(stream => stream.Dispose());

        var driver = CreatePoolingDriver(attachStream.Object, queryStream.Object);
        await using var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);
        Assert.True(PoolManager.Pools.TryAdd(settings.PoolKey, source));
        try
        {
            await using var connection = new YdbConnection(settings);
            await connection.OpenAsync();

            await using var reader = await new YdbCommand("SELECT 1", connection).ExecuteReaderAsync();
            await reader.CloseAsync();
            Assert.Equal(ConnectionState.Broken, connection.State);

            lifecycleAttach.SetResult(false);
            await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await connection.CloseAsync();

            meterProvider.ForceFlush();
            AssertSessionClosed(exportedItems, settings, "client_cancelled");

            var operationFailed = GetMetric(exportedItems, "ydb.client.operation.failed");
            var point = GetOperationFailedPoint(
                operationFailed.GetMetricPoints(), settings, "ExecuteQuery", "ClientCancelled");
            Assert.Equal(1, point.GetSumLong());
        }
        finally
        {
            Assert.True(PoolManager.Pools.TryRemove(settings.PoolKey, out _));
        }
    }

    [Theory]
    [InlineData(false, "pool_idle_timeout")]
    [InlineData(true, "pool_graceful_shutdown")]
    public async Task SessionClosed_WhenPoolClosesIdleSession_ReportsReason(
        bool disposePool,
        string reason)
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.SessionIdleTimeout = 1;
            builder.PoolName = $"ado-metrics-{reason}";
        });
        var lifecycleAttach = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachDisposed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var deleteStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attachStream = new Mock<IServerStream<SessionState>>(MockBehavior.Strict);
        attachStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .Returns(lifecycleAttach.Task);
        attachStream.Setup(stream => stream.Current)
            .Returns(new SessionState { Status = StatusIds.Types.StatusCode.Success });
        attachStream.Setup(stream => stream.Dispose()).Callback(() => attachDisposed.TrySetResult());

        var driver = CreatePoolingDriver(attachStream.Object, deleteSession: () =>
        {
            deleteStarted.TrySetResult();
            lifecycleAttach.TrySetResult(false);
        });
        var source = new PoolingSessionSource<PoolingSession>(
            new PoolingSessionFactory(driver.Object, settings),
            settings);
        try
        {
            var session = await source.OpenSession();
            session.Dispose();

            if (disposePool)
                await source.DisposeAsync();

            await deleteStarted.Task.WaitAsync(TimeSpan.FromSeconds(5));
            await attachDisposed.Task.WaitAsync(TimeSpan.FromSeconds(5));

            meterProvider.ForceFlush();
            AssertSessionClosed(exportedItems, settings, reason);
        }
        finally
        {
            await source.DisposeAsync();
        }
    }

    [Fact]
    public async Task ConnectionPendingRequests()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.CreateSessionTimeout = 5;
            builder.PoolName = "ado-metrics-pending";
        });

        await using var dataSource = new YdbDataSource(settings);
        var firstConn = await dataSource.OpenConnectionAsync();

        var secondConnectionTask = dataSource.OpenConnectionAsync();
        await Task.Yield(); // let secondConnectionTask reach ReportPendingConnectionRequestStart
        meterProvider.ForceFlush();

        var pendingMetric = GetMetric(exportedItems, "ydb.query.session.pending_requests");
        var pendingPoint = GetPoolPoints(pendingMetric.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(1, pendingPoint.GetSumLong());
        Assert.Equal(settings.PoolName, ToDictionary(pendingPoint.Tags)["ydb.query.session.pool.name"]);

        await firstConn.DisposeAsync();
        await using var secondConn = await secondConnectionTask;

        exportedItems.Clear();
        meterProvider.ForceFlush();
        pendingMetric = GetMetric(exportedItems, "ydb.query.session.pending_requests");
        // Counter only increases; cumulative sum does not return to zero when wait ends.
        Assert.True(GetPoolPoints(pendingMetric.GetMetricPoints(), settings.PoolName!).Single().GetSumLong() >= 1);
    }

    [Fact]
    public async Task PoolSizeMaxMin()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MinPoolSize = 2;
            builder.MaxPoolSize = 7;
            builder.PoolName = "ado-metrics-max-min";
        });

        await using var dataSource = new YdbDataSource(settings);
        await using var _ = await dataSource.OpenConnectionAsync();

        meterProvider.ForceFlush();

        var max = GetMetric(exportedItems, "ydb.query.session.max");
        var maxPoint = GetPoolPoints(max.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(7, maxPoint.GetGaugeLastValueLong());
        Assert.Equal(settings.PoolName, ToDictionary(maxPoint.Tags)["ydb.query.session.pool.name"]);

        var min = GetMetric(exportedItems, "ydb.query.session.min");
        var minPoint = GetPoolPoints(min.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(2, minPoint.GetGaugeLastValueLong());
        Assert.Equal(settings.PoolName, ToDictionary(minPoint.Tags)["ydb.query.session.pool.name"]);
    }

    [Fact]
    public async Task ConnectionTimeouts()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.MaxPoolSize = 1;
            builder.CreateSessionTimeout = 1;
            builder.PoolName = "ado-metrics-timeouts";
        });

        await using var dataSource = new YdbDataSource(settings);

        await using var conn = await dataSource.OpenConnectionAsync();

        await Assert.ThrowsAsync<YdbException>(async () => await dataSource.OpenConnectionAsync());

        meterProvider.ForceFlush();

        var metric = GetMetric(exportedItems, "ydb.query.session.timeouts");
        Assert.NotNull(metric);

        var point = GetPoolPoints(metric.GetMetricPoints(), settings.PoolName!).Single();
        Assert.Equal(1, point.GetSumLong());
        Assert.Equal(settings.PoolName, ToDictionary(point.Tags)["ydb.query.session.pool.name"]);
    }

    [Fact]
    public async Task ImplicitSessionSource_DoesNotPublishPoolMetrics()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var settings = CreateConnectionSettings(builder =>
        {
            builder.EnableImplicitSession = true;
            builder.PoolName = "ado-metrics-implicit-query-error";
        });
        var queryStream = new Mock<IServerStream<ExecuteQueryResponsePart>>(MockBehavior.Strict);
        queryStream.SetupSequence(stream => stream.MoveNextAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        queryStream.Setup(stream => stream.Current)
            .Returns(new ExecuteQueryResponsePart { Status = StatusIds.Types.StatusCode.SessionBusy });

        var driver = CreateMockDriver();
        driver.Setup(d => d.ServerStreamCall(
                QueryService.ExecuteQueryMethod,
                It.IsAny<ExecuteQueryRequest>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(queryStream.Object);

        await using var source = new ImplicitSessionSource(driver.Object, settings);
        Assert.True(PoolManager.Pools.TryAdd(settings.PoolKey, source));
        try
        {
            await using var connection = new YdbConnection(settings);
            await connection.OpenAsync();

            await Assert.ThrowsAsync<YdbException>(() =>
                new YdbCommand("SELECT 1", connection).ExecuteReaderAsync());

            meterProvider.ForceFlush();
            AssertNoPoolMetricsForPool(exportedItems, settings.PoolName!);
        }
        finally
        {
            Assert.True(PoolManager.Pools.TryRemove(settings.PoolKey, out _));
        }
    }

    private const string RetryDurationMetric = "ydb.client.retry.duration";
    private const string RetryAttemptsMetric = "ydb.client.retry.attempts";

    [Fact]
    public async Task RetryMetrics_FirstTrySuccess_RecordsOneAttempt()
    {
        var operationName = NewOperationName();
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default, operationName);
        await executor.ExecuteAsync(_ => Task.CompletedTask);

        meterProvider.ForceFlush();

        var attemptsPoint = SinglePointForOperation(exportedItems, RetryAttemptsMetric, operationName);
        Assert.Equal(1, attemptsPoint.GetHistogramCount());
        Assert.Equal(1, attemptsPoint.GetHistogramSum());

        var durationPoint = SinglePointForOperation(exportedItems, RetryDurationMetric, operationName);
        Assert.Equal(1, durationPoint.GetHistogramCount());
        Assert.True(durationPoint.GetHistogramSum() >= 0);
    }

    [Fact]
    public async Task RetryMetrics_WithRetries_RecordsTotalAttempts()
    {
        var operationName = NewOperationName();
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        // MaxAttempts=5 → up to 4 retries; we'll succeed on the 3rd call (after 2 retries).
        var policy = new YdbRetryPolicy(new YdbRetryPolicyConfig
        {
            MaxAttempts = 5,
            FastBackoffBaseMs = 1,
            FastCapBackoffMs = 1
        });
        var executor = new YdbRetryPolicyExecutor(policy, operationName);

        var calls = 0;
        await executor.ExecuteAsync(_ =>
        {
            calls++;
            if (calls < 3)
                throw new YdbException(StatusCode.Aborted, "retry me");
            return Task.CompletedTask;
        });

        Assert.Equal(3, calls);

        meterProvider.ForceFlush();

        var attemptsPoint = SinglePointForOperation(exportedItems, RetryAttemptsMetric, operationName);
        Assert.Equal(1, attemptsPoint.GetHistogramCount());
        Assert.Equal(3, attemptsPoint.GetHistogramSum());
    }

    [Fact]
    public async Task RetryMetrics_NonRetryableError_StillRecorded()
    {
        var operationName = NewOperationName();
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default, operationName);
        await Assert.ThrowsAsync<YdbException>(() =>
            executor.ExecuteAsync(_ => throw new YdbException(StatusCode.Unauthorized, "no")));

        meterProvider.ForceFlush();

        var attemptsPoint = SinglePointForOperation(exportedItems, RetryAttemptsMetric, operationName);
        Assert.Equal(1, attemptsPoint.GetHistogramSum());

        var durationPoint = SinglePointForOperation(exportedItems, RetryDurationMetric, operationName);
        Assert.Equal(1, durationPoint.GetHistogramCount());
    }

    [Fact]
    public async Task RetryMetrics_RetriesExhausted_RecordsAllAttempts()
    {
        var operationName = NewOperationName();
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var policy = new YdbRetryPolicy(new YdbRetryPolicyConfig
        {
            MaxAttempts = 3,
            FastBackoffBaseMs = 1,
            FastCapBackoffMs = 1
        });
        var executor = new YdbRetryPolicyExecutor(policy, operationName);

        await Assert.ThrowsAsync<YdbException>(() =>
            executor.ExecuteAsync(_ => throw new YdbException(StatusCode.Aborted, "always fails")));

        meterProvider.ForceFlush();

        var attemptsPoint = SinglePointForOperation(exportedItems, RetryAttemptsMetric, operationName);
        // MaxAttempts=3 ⇒ initial + 2 retries ⇒ 3 calls total
        Assert.Equal(3, attemptsPoint.GetHistogramSum());
    }

    [Fact]
    public async Task RetryMetrics_NoOperationName_OmitsOperationNameTag()
    {
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default);
        await executor.ExecuteAsync(_ => Task.CompletedTask);

        meterProvider.ForceFlush();

        foreach (var name in new[] { RetryAttemptsMetric, RetryDurationMetric })
        {
            var metric = GetMetric(exportedItems, name);
            // At least one point with no operation.name tag must be present (the call we just made).
            Assert.Contains(EnumeratePoints(metric),
                point => !ToDictionary(point.Tags).ContainsKey("operation.name"));
        }
    }

    [Fact]
    public async Task RetryMetrics_WithOperationName_TagsBothMetrics()
    {
        var operationName = NewOperationName();
        var exportedItems = new List<Metric>();
        using var meterProvider = CreateMeterProvider(exportedItems);

        var executor = new YdbRetryPolicyExecutor(YdbRetryPolicy.Default, operationName);
        await executor.ExecuteAsync(_ => Task.CompletedTask);

        meterProvider.ForceFlush();

        foreach (var name in new[] { RetryAttemptsMetric, RetryDurationMetric })
        {
            var point = SinglePointForOperation(exportedItems, name, operationName);
            Assert.Equal(operationName, ToDictionary(point.Tags)["operation.name"]);
        }
    }

    private static MetricPoint SinglePointForOperation(
        List<Metric> exportedItems,
        string metricName,
        string operationName)
    {
        var metric = GetMetric(exportedItems, metricName);
        return EnumeratePoints(metric)
            .Single(p => ToDictionary(p.Tags).GetValueOrDefault("operation.name") as string == operationName);
    }

    private static IEnumerable<MetricPoint> EnumeratePoints(Metric metric)
    {
        foreach (var point in metric.GetMetricPoints())
            yield return point;
    }

    private static string NewOperationName() => "TestOp." + Guid.NewGuid().ToString("N");

    private static readonly string[] PoolScopedMetricNames =
    [
        "ydb.query.session.count",
        "ydb.query.session.max",
        "ydb.query.session.min",
        "ydb.query.session.timeouts",
        "ydb.query.session.pending_requests",
        "ydb.query.session.create_time",
        "ydb.query.session.closed"
    ];

    private static void AssertNoPoolMetricsForPool(List<Metric> exportedItems, string poolName)
    {
        foreach (var metric in exportedItems.Where(m => PoolScopedMetricNames.Contains(m.Name)))
        {
            foreach (var point in metric.GetMetricPoints())
            {
                if (ToDictionary(point.Tags).GetValueOrDefault("ydb.query.session.pool.name") as string == poolName)
                {
                    Assert.Fail(
                        $"Implicit session must not publish pool metric '{metric.Name}' for pool '{poolName}'.");
                }
            }
        }
    }

    private static MeterProvider CreateMeterProvider(List<Metric> exportedItems) =>
        global::OpenTelemetry.Sdk.CreateMeterProviderBuilder()
            .AddYdbAdo()
            .AddInMemoryExporter(exportedItems)
            .Build();

    private static YdbConnectionStringBuilder CreateConnectionSettings(
        Action<YdbConnectionStringBuilder>? configure = null)
    {
        var settings = new YdbConnectionStringBuilder(TestUtils.ConnectionString)
        {
            PoolName = BaseConnectionSettings.PoolName
        };
        configure?.Invoke(settings);
        return settings;
    }

    private static Metric GetMetric(List<Metric> exportedItems, string name) =>
        exportedItems.Single(m => m.Name == name);

    private static Mock<IDriver> CreateMockDriver()
    {
        var driver = new Mock<IDriver>(MockBehavior.Strict);
        driver.SetupGet(d => d.LoggerFactory).Returns(TestUtils.LoggerFactory);
        driver.Setup(d => d.DisposeAsync()).Returns(ValueTask.CompletedTask);
        return driver;
    }

    private static Mock<IDriver> CreatePoolingDriver(
        IServerStream<SessionState> attachStream,
        IServerStream<ExecuteQueryResponsePart>? queryStream = null,
        Action? deleteSession = null)
    {
        var driver = CreateMockDriver();
        driver.Setup(d => d.UnaryCall(
                QueryService.CreateSessionMethod,
                It.IsAny<CreateSessionRequest>(),
                It.Is<GrpcRequestSettings>(s => s.ClientCapabilities.Contains("session-balancer"))))
            .ReturnsAsync(new CreateSessionResponse
            {
                Status = StatusIds.Types.StatusCode.Success,
                SessionId = MockSessionId,
                NodeId = MockNodeId
            });
        driver.Setup(d => d.ServerStreamCall(
                QueryService.AttachSessionMethod,
                It.Is<AttachSessionRequest>(r => r.SessionId == MockSessionId),
                It.Is<GrpcRequestSettings>(s => s.NodeId == MockNodeId)))
            .ReturnsAsync(attachStream);
        driver.Setup(d => d.UnaryCall(
                QueryService.DeleteSessionMethod,
                It.Is<DeleteSessionRequest>(r => r.SessionId == MockSessionId),
                It.Is<GrpcRequestSettings>(s => s.NodeId == MockNodeId)))
            .Callback(() => deleteSession?.Invoke())
            .ReturnsAsync(new DeleteSessionResponse { Status = StatusIds.Types.StatusCode.Success });
        driver.Setup(d => d.PessimizeNode(MockNodeId));

        if (queryStream != null)
        {
            driver.Setup(d => d.ServerStreamCall(
                    QueryService.ExecuteQueryMethod,
                    It.IsAny<ExecuteQueryRequest>(),
                    It.Is<GrpcRequestSettings>(s => s.NodeId == MockNodeId)))
                .ReturnsAsync(queryStream);
        }

        return driver;
    }

    private static void AssertSessionClosed(
        List<Metric> exportedItems,
        YdbConnectionStringBuilder settings,
        string reason)
    {
        var metric = GetMetric(exportedItems, "ydb.query.session.closed");
        var point = GetPoolPoints(metric.GetMetricPoints(), settings.PoolName!).Single();
        var tags = ToDictionary(point.Tags);

        Assert.Equal(1, point.GetSumLong());
        Assert.Equal(settings.PoolName, tags["ydb.query.session.pool.name"]);
        Assert.Equal(reason, tags["reason"]);
    }

    private static void AssertSessionNotClosed(List<Metric> exportedItems, string poolName)
    {
        var metric = exportedItems.SingleOrDefault(m => m.Name == "ydb.query.session.closed");
        if (metric != null)
            Assert.Empty(GetPoolPoints(metric.GetMetricPoints(), poolName));
    }

    private static IEnumerable<MetricPoint> GetConnectionCountPoints(MetricPointsAccessor points, string poolName)
    {
        foreach (var point in points)
        {
            var tagPoolName = ToDictionary(point.Tags).GetValueOrDefault("ydb.query.session.pool.name") as string;
            if (string.Equals(tagPoolName, poolName, StringComparison.Ordinal))
            {
                yield return point;
            }
        }
    }

    private static MetricPoint GetPoint(IEnumerable<MetricPoint> points, string state)
    {
        foreach (var point in points)
        {
            foreach (var tag in point.Tags)
            {
                if (tag.Key == "ydb.query.session.state" && (string?)tag.Value == state)
                {
                    return point;
                }
            }
        }

        Assert.Fail($"Point with state '{state}' not found");
        throw new UnreachableException();
    }

    private static MetricPoint GetOperationFailedPoint(
        MetricPointsAccessor points,
        YdbConnectionStringBuilder settings,
        string operationName,
        string statusCode)
    {
        foreach (var point in points)
        {
            var tags = ToDictionary(point.Tags);
            if (string.Equals(tags.GetValueOrDefault("database") as string, settings.Database,
                    StringComparison.Ordinal) &&
                string.Equals(tags.GetValueOrDefault("endpoint") as string, EndpointFor(settings),
                    StringComparison.Ordinal) &&
                string.Equals(tags.GetValueOrDefault("operation.name") as string, operationName,
                    StringComparison.Ordinal) &&
                string.Equals(tags.GetValueOrDefault("status_code") as string, statusCode, StringComparison.Ordinal))
            {
                return point;
            }
        }

        Assert.Fail($"Point for operation '{operationName}' with status '{statusCode}' not found");
        throw new UnreachableException();
    }

    private static Dictionary<string, object?> ToDictionary(ReadOnlyTagCollection tags)
    {
        var dict = new Dictionary<string, object?>();
        foreach (var tag in tags)
        {
            dict[tag.Key] = tag.Value;
        }

        return dict;
    }

    private static IEnumerable<MetricPoint> GetPoolPoints(MetricPointsAccessor points, string poolName)
    {
        foreach (var point in points)
        {
            var tagPoolName = ToDictionary(point.Tags).GetValueOrDefault("ydb.query.session.pool.name") as string;
            if (string.Equals(tagPoolName, poolName, StringComparison.Ordinal))
                yield return point;
        }
    }

    private static IEnumerable<MetricPoint> GetFilteredPoints(MetricPointsAccessor points)
    {
        foreach (var point in points)
        {
            var tags = ToDictionary(point.Tags);
            if (string.Equals(tags.GetValueOrDefault("database") as string, BaseConnectionSettings.Database,
                    StringComparison.Ordinal) &&
                string.Equals(tags.GetValueOrDefault("endpoint") as string, EndpointFor(BaseConnectionSettings),
                    StringComparison.Ordinal))
            {
                yield return point;
            }
        }
    }
}
