using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using Grpc.Core;
using Moq;
using Xunit;
using Ydb.Coordination;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Settings;

namespace Ydb.Sdk.Coordination.Tests;

using CoordinationStream = IBidirectionalStream<SessionRequest, SessionResponse>;

public class CoordinationUnitTests
{
    private static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task CreateSemaphore_WhenSessionStarted_SendsCreateSemaphoreRequest()
    {
        var scenario = new CoordinationStreamScenario();
        scenario.HandleRequest = request => request.CreateSemaphore != null
            ? CreateSemaphoreResult(request.CreateSemaphore.ReqId)
            : null;

        await using var session = CreateSession(scenario);
        var semaphore = session.Semaphore("semaphore");
        var data = new byte[] { 0x01, 0x02, 0x03 };

        await semaphore.Create(10, data).WaitAsync(TestTimeout);

        var createRequest = Assert.Single(scenario.WrittenRequests,
            request => request.CreateSemaphore != null).CreateSemaphore;

        Assert.Equal("semaphore", createRequest.Name);
        Assert.Equal((ulong)10, createRequest.Limit);
        Assert.Equal(data, createRequest.Data.ToByteArray());
        Assert.Equal((ulong)1, createRequest.ReqId);
        Assert.Equal((ulong)42, session.SessionId());
        _ = await scenario.Close(session).WaitAsync(TestTimeout);
    }
    
    [Fact]
    public async Task AcquireSemaphore_WhenSessionStarted_SendsSessionStartAndAcquireRequest()
    {
        var scenario = new CoordinationStreamScenario();
        scenario.HandleRequest = request =>
        {
            if (request.AcquireSemaphore != null)
            {
                return AcquireSemaphoreResult(request.AcquireSemaphore.ReqId, acquired: true);
            }

            return request.ReleaseSemaphore != null
                ? ReleaseSemaphoreResult(request.ReleaseSemaphore.ReqId)
                : null;
        };

        await using var session = CreateSession(scenario);
        var semaphore = session.Semaphore("lease");
        var data = "owner"u8.ToArray();

        var lease = await semaphore.Acquire(
            2,
            isEphemeral: true,
            data,
            TimeSpan.FromSeconds(3)).WaitAsync(TestTimeout);

        var sessionStart = Assert.Single(scenario.WrittenRequests,
            request => request.SessionStart != null).SessionStart;
        Assert.Equal((ulong)0, sessionStart.SessionId);
        Assert.Equal("/local/unit-test", sessionStart.Path);
        Assert.NotEmpty(sessionStart.ProtectionKey.ToByteArray());

        var acquireRequest = Assert.Single(scenario.WrittenRequests,
            request => request.AcquireSemaphore != null).AcquireSemaphore;
        Assert.Equal("lease", acquireRequest.Name);
        Assert.Equal((ulong)2, acquireRequest.Count);
        Assert.True(acquireRequest.Ephemeral);
        Assert.Equal(data, acquireRequest.Data.ToByteArray());
        Assert.Equal((ulong)3000, acquireRequest.TimeoutMillis);
        Assert.Equal((ulong)1, acquireRequest.ReqId);

        await lease.Release().WaitAsync(TestTimeout);
        _ = await scenario.Close(session).WaitAsync(TestTimeout);
    }
    
    [Fact]
    public async Task SessionRecovery_WhenStreamCloses_ReusesSessionIdAndProtectionKey()
    {
        var firstStream = new Mock<CoordinationStream>();
        var recoveryStream = new Mock<CoordinationStream>();
        var firstRequests = new ConcurrentQueue<SessionRequest>();
        var recoveryRequests = new ConcurrentQueue<SessionRequest>();
        var recoverySessionStartWritten = new TaskCompletionSource();
        var recoverySessionStopWritten = new TaskCompletionSource<bool>();
        var recoveryCurrent = SessionStarted(42);

        firstStream.Setup(stream => stream.Write(It.IsAny<SessionRequest>()))
            .Returns<SessionRequest>(request =>
            {
                firstRequests.Enqueue(request);

                return Task.CompletedTask;
            });
        firstStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .ReturnsAsync(false);
        firstStream.Setup(stream => stream.Current)
            .Returns(SessionStarted(42));
        firstStream.Setup(stream => stream.RequestStreamComplete())
            .Returns(Task.CompletedTask);

        recoveryStream.Setup(stream => stream.Write(It.IsAny<SessionRequest>()))
            .Returns<SessionRequest>(request =>
            {
                recoveryRequests.Enqueue(request);
                if (request.SessionStart != null)
                {
                    recoverySessionStartWritten.SetResult();
                }

                if (request.SessionStop != null)
                {
                    recoveryCurrent = SessionStopped(42);
                    recoverySessionStopWritten.SetResult(true);
                }

                return Task.CompletedTask;
            });
        recoveryStream.SetupSequence(stream => stream.MoveNextAsync())
            .ReturnsAsync(true)
            .Returns(recoverySessionStopWritten.Task)
            .ReturnsAsync(false);
        recoveryStream.Setup(stream => stream.Current)
            .Returns(() => recoveryCurrent);
        recoveryStream.Setup(stream => stream.RequestStreamComplete())
            .Returns(Task.CompletedTask);

        var driver = new Mock<IDriver>();
        driver.SetupSequence(d => d.BidirectionalStreamCall(
                It.IsAny<Method<SessionRequest, SessionResponse>>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(firstStream.Object)
            .ReturnsAsync(recoveryStream.Object);
        driver.Setup(d => d.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(d => d.Database).Returns("/local");

        await using var session = new CoordinationSession(
            driver.Object,
            "/local/unit-test",
            SessionOptions.Default,
            null);

        await recoverySessionStartWritten.Task.WaitAsync(TestTimeout);

        var firstStart = Assert.Single(firstRequests,
            request => request.SessionStart != null).SessionStart;
        var recoveryStart = Assert.Single(recoveryRequests,
            request => request.SessionStart != null).SessionStart;

        Assert.Equal((ulong)42, recoveryStart.SessionId);
        Assert.Equal(firstStart.ProtectionKey.ToByteArray(), recoveryStart.ProtectionKey.ToByteArray());
        Assert.Equal(firstStart.Path, recoveryStart.Path);
        Assert.Equal(firstStart.SeqNo + 1, recoveryStart.SeqNo);

        await session.Close().WaitAsync(TestTimeout);
    }
    
    [Fact]
    public async Task Ping_WhenReceived_RepliesWithPong()
    {
        var scenario = new CoordinationStreamScenario();

        await using var session = CreateSession(scenario);

        await WaitUntil(() => scenario.WrittenRequests.Any(request => request.SessionStart != null));

        scenario.Enqueue(Ping(123));

        await WaitUntil(() => scenario.WrittenRequests.Any(request => request.Pong != null));

        var pong = Assert.Single(scenario.WrittenRequests,
            request => request.Pong != null).Pong;
        Assert.Equal((ulong)123, pong.Opaque);

        _ = await scenario.Close(session).WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task TryAcquire_WhenSemaphoreIsBusy_ReturnsNull()
    {
        var scenario = new CoordinationStreamScenario();
        scenario.HandleRequest = request => request.AcquireSemaphore != null
            ? AcquireSemaphoreResult(request.AcquireSemaphore.ReqId, acquired: false)
            : null;

        await using var session = CreateSession(scenario);
        var semaphore = session.Semaphore("busy");

        var lease = await semaphore.TryAcquire(3, isEphemeral: false, data: null).WaitAsync(TestTimeout);

        Assert.Null(lease);
        var acquireRequest = Assert.Single(scenario.WrittenRequests,
            request => request.AcquireSemaphore != null).AcquireSemaphore;
        Assert.Equal("busy", acquireRequest.Name);
        Assert.Equal((ulong)3, acquireRequest.Count);
        Assert.False(acquireRequest.Ephemeral);
        Assert.Equal((ulong)0, acquireRequest.TimeoutMillis);
        Assert.DoesNotContain(scenario.WrittenRequests, request => request.ReleaseSemaphore != null);
        _ = await scenario.Close(session).WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task MutexLock_WhenDisposed_ReleasesSemaphore()
    {
        var scenario = new CoordinationStreamScenario();
        scenario.HandleRequest = request =>
        {
            if (request.AcquireSemaphore != null)
            {
                return AcquireSemaphoreResult(request.AcquireSemaphore.ReqId, acquired: true);
            }

            return request.ReleaseSemaphore != null
                ? ReleaseSemaphoreResult(request.ReleaseSemaphore.ReqId)
                : null;
        };

        await using var session = CreateSession(scenario);
        var mutex = session.Mutex("jobLock");

        await using (await mutex.Lock(CancellationToken.None).WaitAsync(TestTimeout))
        {
        }

        var acquireRequest = Assert.Single(scenario.WrittenRequests,
            request => request.AcquireSemaphore != null).AcquireSemaphore;
        Assert.Equal("jobLock", acquireRequest.Name);
        Assert.Equal(ulong.MaxValue, acquireRequest.Count);
        Assert.True(acquireRequest.Ephemeral);

        var releaseRequest = Assert.Single(scenario.WrittenRequests,
            request => request.ReleaseSemaphore != null).ReleaseSemaphore;
        Assert.Equal("jobLock", releaseRequest.Name);
        Assert.Equal((ulong)2, releaseRequest.ReqId);
        _ = await scenario.Close(session).WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task WatchSemaphore_WhenDataChanged_ReturnsUpdatedDescription()
    {
        var scenario = new CoordinationStreamScenario();
        ulong watchReqId = 0;
        var describeCount = 0;

        scenario.HandleRequest = request =>
        {
            if (request.DescribeSemaphore == null)
            {
                return null;
            }

            describeCount++;
            watchReqId = request.DescribeSemaphore.ReqId;

            return DescribeSemaphoreResult(
                request.DescribeSemaphore.ReqId,
                "config",
                describeCount == 1 ? "v1"u8.ToArray() : "v2"u8.ToArray(),
                watchAdded: true);
        };

        await using var session = CreateSession(scenario);
        var semaphore = session.Semaphore("config");

        var watch = await semaphore.WatchSemaphore(
            DescribeSemaphoreMode.DataOnly,
            WatchSemaphoreMode.WatchData).WaitAsync(TestTimeout);

        Assert.Equal("v1"u8.ToArray(), watch.Initial.Data);

        await using var updates = watch.Updates.GetAsyncEnumerator();
        var moveNext = updates.MoveNextAsync().AsTask();
        Assert.False(moveNext.IsCompleted);

        scenario.Enqueue(DescribeSemaphoreChanged(watchReqId, dataChanged: true, ownersChanged: false));

        Assert.True(await moveNext.WaitAsync(TestTimeout));
        Assert.Equal("v2"u8.ToArray(), updates.Current.Data);
        Assert.Equal(2, describeCount);
        _ = await scenario.Close(session).WaitAsync(TestTimeout);
    }

    [Fact]
    public async Task Election_WhenCampaignProclaimAndQueryLeader_UsesSemaphoreOwnerData()
    {
        var scenario = new CoordinationStreamScenario();
        var leaderData = "candidate"u8.ToArray();

        scenario.HandleRequest = request =>
        {
            if (request.AcquireSemaphore != null)
            {
                leaderData = request.AcquireSemaphore.Data.ToByteArray();
                return AcquireSemaphoreResult(request.AcquireSemaphore.ReqId, acquired: true);
            }

            if (request.UpdateSemaphore != null)
            {
                leaderData = request.UpdateSemaphore.Data.ToByteArray();
                return UpdateSemaphoreResult(request.UpdateSemaphore.ReqId);
            }

            if (request.DescribeSemaphore != null)
            {
                return DescribeSemaphoreResult(
                    request.DescribeSemaphore.ReqId,
                    "leader",
                    data: null,
                    watchAdded: false,
                    owners: new[]
                    {
                        new SemaphoreSession
                        {
                            SessionId = scenario.SessionId,
                            Count = 1,
                            Data = ByteString.CopyFrom(leaderData),
                            OrderId = 7
                        }
                    });
            }

            return request.ReleaseSemaphore != null
                ? ReleaseSemaphoreResult(request.ReleaseSemaphore.ReqId)
                : null;
        };

        await using var session = CreateSession(scenario);
        var election = session.Election("leader");

        await using var leadership =
            await election.Campaign("candidate"u8.ToArray()).WaitAsync(TestTimeout);

        await leadership.Proclaim("host:2136"u8.ToArray()).WaitAsync(TestTimeout);

        var leader = await election.Leader().WaitAsync(TestTimeout);

        Assert.NotNull(leader);
        Assert.Equal("host:2136"u8.ToArray(), leader.Data);

        Assert.Contains(scenario.WrittenRequests, request =>
            request.AcquireSemaphore != null &&
            request.AcquireSemaphore.Name == "leader" &&
            request.AcquireSemaphore.Count == 1 &&
            !request.AcquireSemaphore.Ephemeral);
        Assert.Contains(scenario.WrittenRequests, request =>
            request.UpdateSemaphore != null &&
            request.UpdateSemaphore.Name == "leader" &&
            request.UpdateSemaphore.Data.ToByteArray().SequenceEqual("host:2136"u8.ToArray()));
        await leadership.Resign().WaitAsync(TestTimeout);

        _ = await scenario.Close(session).WaitAsync(TestTimeout);
    }

    private static CoordinationSession CreateSession(CoordinationStreamScenario scenario)
    {
        var driver = new Mock<IDriver>();
        driver.Setup(d => d.BidirectionalStreamCall(
                It.IsAny<Method<SessionRequest, SessionResponse>>(),
                It.IsAny<GrpcRequestSettings>()))
            .ReturnsAsync(scenario.Stream);
        driver.Setup(d => d.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(d => d.Database).Returns("/local");

        return new CoordinationSession(
            driver.Object,
            "/local/unit-test",
            SessionOptions.Default,
            null);
    }

    private static SessionResponse SessionStarted(ulong sessionId) => new()
    {
        SessionStarted = new SessionResponse.Types.SessionStarted
        {
            SessionId = sessionId
        }
    };
    
    private static SessionResponse Ping(ulong opaque) => new()
    {
        Ping = new SessionResponse.Types.PingPong
        {
            Opaque = opaque
        }
    };

    private static SessionResponse SessionStopped(ulong sessionId) => new()
    {
        SessionStopped = new SessionResponse.Types.SessionStopped
        {
            SessionId = sessionId
        }
    };

    private static SessionResponse CreateSemaphoreResult(ulong reqId) => new()
    {
        CreateSemaphoreResult = new SessionResponse.Types.CreateSemaphoreResult
        {
            ReqId = reqId
        }
    };

    private static SessionResponse UpdateSemaphoreResult(ulong reqId) => new()
    {
        UpdateSemaphoreResult = new SessionResponse.Types.UpdateSemaphoreResult
        {
            ReqId = reqId
        }
    };

    private static SessionResponse AcquireSemaphoreResult(ulong reqId, bool acquired) => new()
    {
        AcquireSemaphoreResult = new SessionResponse.Types.AcquireSemaphoreResult
        {
            ReqId = reqId,
            Acquired = acquired
        }
    };

    private static SessionResponse ReleaseSemaphoreResult(ulong reqId) => new()
    {
        ReleaseSemaphoreResult = new SessionResponse.Types.ReleaseSemaphoreResult
        {
            ReqId = reqId
        }
    };

    private static SessionResponse DescribeSemaphoreResult(
        ulong reqId,
        string name,
        byte[]? data,
        bool watchAdded,
        IEnumerable<SemaphoreSession>? owners = null,
        IEnumerable<SemaphoreSession>? waiters = null)
    {
        var description = new Ydb.Coordination.SemaphoreDescription
        {
            Name = name,
            Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
            Count = (ulong)(owners?.Sum(owner => (long)owner.Count) ?? 0),
            Limit = 10,
            Ephemeral = false
        };

        if (owners != null)
        {
            description.Owners.Add(owners);
        }

        if (waiters != null)
        {
            description.Waiters.Add(waiters);
        }

        return new SessionResponse
        {
            DescribeSemaphoreResult = new SessionResponse.Types.DescribeSemaphoreResult
            {
                ReqId = reqId,
                WatchAdded = watchAdded,
                SemaphoreDescription = description
            }
        };
    }

    private static SessionResponse DescribeSemaphoreChanged(
        ulong reqId,
        bool dataChanged,
        bool ownersChanged) => new()
    {
        DescribeSemaphoreChanged = new SessionResponse.Types.DescribeSemaphoreChanged
        {
            ReqId = reqId,
            DataChanged = dataChanged,
            OwnersChanged = ownersChanged
        }
    };
    
    private static async Task WaitUntil(Func<bool> predicate)
    {
        using var cts = new CancellationTokenSource(TestTimeout);
        while (!predicate())
        {
            await Task.Delay(10, cts.Token);
        }
    }

    private sealed class CoordinationStreamScenario
    {
        private readonly Mock<CoordinationStream> _mockStream = new();
        private readonly Channel<SessionResponse> _responses = Channel.CreateUnbounded<SessionResponse>(
            new UnboundedChannelOptions
            {
                SingleReader = true,
                AllowSynchronousContinuations = false
            });

        private SessionResponse _current = new();

        public CoordinationStreamScenario()
        {
            _mockStream.Setup(stream => stream.Write(It.IsAny<SessionRequest>()))
                .Returns<SessionRequest>(request =>
                {
                    Written.Enqueue(request);

                    var response = HandleInfrastructureRequest(request) ?? HandleRequest?.Invoke(request);
                    if (response != null)
                    {
                        Enqueue(response);
                    }

                    return Task.CompletedTask;
                });

            _mockStream.Setup(stream => stream.MoveNextAsync())
                .Returns(async () =>
                {
                    while (await _responses.Reader.WaitToReadAsync())
                    {
                        if (_responses.Reader.TryRead(out var response))
                        {
                            _current = response;
                            return true;
                        }
                    }

                    return false;
                });

            _mockStream.Setup(stream => stream.Current).Returns(() => _current);
            _mockStream.Setup(stream => stream.RequestStreamComplete()).Returns(() =>
            {
                _responses.Writer.TryComplete();
                return Task.CompletedTask;
            });
        }

        public ulong SessionId { get; } = 42;

        public CoordinationStream Stream => _mockStream.Object;

        public ConcurrentQueue<SessionRequest> Written { get; } = new();

        public IReadOnlyList<SessionRequest> WrittenRequests => Written.ToArray();

        public Func<SessionRequest, SessionResponse?>? HandleRequest { get; set; }

        public void Enqueue(SessionResponse response) => _responses.Writer.TryWrite(response);

        public async Task<bool> Close(CoordinationSession session)
        {
            await session.Close();
            return true;
        }

        private SessionResponse? HandleInfrastructureRequest(SessionRequest request)
        {
            if (request.SessionStart != null)
            {
                return SessionStarted(SessionId);
            }

            return request.SessionStop != null
                ? SessionStopped(SessionId)
                : null;
        }
    }
}
