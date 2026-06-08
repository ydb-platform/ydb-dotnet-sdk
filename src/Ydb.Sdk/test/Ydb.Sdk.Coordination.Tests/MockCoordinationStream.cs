using System.Collections.Concurrent;
using System.Threading.Channels;
using Google.Protobuf;
using Grpc.Core;
using Moq;
using Ydb.Coordination;

namespace Ydb.Sdk.Coordination.Tests;

using Stream = IBidirectionalStream<SessionRequest, SessionResponse>;

/// <summary>
/// In-memory bidirectional stream + IDriver mock for CoordinationSession tests.
/// </summary>
/// <remarks>
/// <para>Records every <see cref="SessionRequest"/> written by the SUT and lets the test feed back
/// <see cref="SessionResponse"/> messages via <see cref="Enqueue"/>. By default the scenario reacts
/// to infrastructure messages (<c>SessionStart</c> → <c>SessionStarted</c>, <c>SessionStop</c> →
/// <c>SessionStopped</c>) so each unit test only needs to wire up the responses it cares about.</para>
///
/// <para>Multiple streams can be registered via <see cref="AddStream"/> to simulate a reconnect. The
/// driver mock returns them in registration order.</para>
/// </remarks>
internal sealed class MockCoordinationStream
{
    public const ulong DefaultSessionId = 42;

    private readonly List<Mock<Stream>> _streams = new();
    private readonly List<StreamState> _states = new();
    private int _nextStreamIndex = -1;

    public MockCoordinationStream()
    {
        AddStream();
    }

    public ulong SessionId { get; set; } = DefaultSessionId;

    public Func<SessionRequest, SessionResponse?>? HandleRequest { get; set; }

    /// <summary>Add an extra stream to the queue — used to simulate reconnects.</summary>
    public void AddStream()
    {
        var mock = new Mock<Stream>();
        var state = new StreamState();
        var localIndex = _streams.Count;

        mock.Setup(s => s.Write(It.IsAny<SessionRequest>()))
            .Returns<SessionRequest>(request =>
            {
                state.Written.Enqueue(request);

                // User-provided handler wins. Infrastructure (SessionStart/Stop, CreateSemaphore)
                // is a default fallback so individual tests don't need to repeat it.
                var response = HandleRequest?.Invoke(request) ?? HandleInfrastructure(request);
                if (response is not null)
                    state.Responses.Writer.TryWrite(response);

                return Task.CompletedTask;
            });

        mock.Setup(s => s.MoveNextAsync())
            .Returns(async () =>
            {
                while (await state.Responses.Reader.WaitToReadAsync())
                {
                    if (state.Responses.Reader.TryRead(out var resp))
                    {
                        state.Current = resp;
                        return true;
                    }
                }
                return false;
            });

        mock.Setup(s => s.Current).Returns(() => state.Current);

        mock.Setup(s => s.RequestStreamComplete()).Returns(() =>
        {
            state.Responses.Writer.TryComplete();
            return Task.CompletedTask;
        });

        mock.Setup(s => s.AuthToken()).Returns(ValueTask.FromResult<string?>(null));

        _streams.Add(mock);
        _states.Add(state);
        _ = localIndex; // available for future hooks
    }

    /// <summary>Push a response into the currently-active stream (the one the SUT is reading from).</summary>
    public void Enqueue(SessionResponse response, int? streamIndex = null)
    {
        var index = streamIndex ?? Math.Max(0, _nextStreamIndex);
        _states[index].Responses.Writer.TryWrite(response);
    }

    /// <summary>Forcibly closes the current stream — simulates a transport drop.</summary>
    public void BreakStream(int? streamIndex = null)
    {
        var index = streamIndex ?? Math.Max(0, _nextStreamIndex);
        _states[index].Responses.Writer.TryComplete();
    }

    /// <summary>Snapshot of requests written so far on a given stream (default: current).</summary>
    public IReadOnlyList<SessionRequest> Written(int? streamIndex = null)
    {
        var index = streamIndex ?? Math.Max(0, _nextStreamIndex);
        return _states[index].Written.ToArray();
    }

    /// <summary>Configure an <see cref="IDriver"/> mock that hands out the prepared streams in order.</summary>
    public Mock<IDriver> SetupDriver()
    {
        var driver = new Mock<IDriver>();
        driver.Setup(d => d.LoggerFactory).Returns(Utils.LoggerFactory);
        driver.Setup(d => d.Database).Returns("/local");
        driver.Setup(d => d.BidirectionalStreamCall(
                It.IsAny<Method<SessionRequest, SessionResponse>>(),
                It.IsAny<GrpcRequestSettings>()))
            .Returns(() =>
            {
                var idx = Interlocked.Increment(ref _nextStreamIndex);
                if (idx >= _streams.Count)
                    throw new InvalidOperationException(
                        $"Test scenario ran out of prepared streams (requested {idx + 1}, prepared {_streams.Count})");
                return ValueTask.FromResult(_streams[idx].Object);
            });
        return driver;
    }

    private SessionResponse? HandleInfrastructure(SessionRequest request) =>
        request.RequestCase switch
        {
            SessionRequest.RequestOneofCase.SessionStart => SessionStarted(SessionId),
            SessionRequest.RequestOneofCase.SessionStop => SessionStopped(SessionId),
            // High-level recipes invoke CreateSemaphore idempotently during open; auto-reply so
            // recipe tests don't need to special-case it. Tests that care can still inspect Written().
            SessionRequest.RequestOneofCase.CreateSemaphore =>
                CreateSemaphoreResult(request.CreateSemaphore.ReqId),
            _ => null
        };

    // ----------------------------------------------------------------------
    // Static helpers for building responses
    // ----------------------------------------------------------------------

    public static SessionResponse SessionStarted(ulong sessionId) => new()
    {
        SessionStarted = new SessionResponse.Types.SessionStarted { SessionId = sessionId }
    };

    public static SessionResponse SessionStopped(ulong sessionId) => new()
    {
        SessionStopped = new SessionResponse.Types.SessionStopped { SessionId = sessionId }
    };

    public static SessionResponse Ping(ulong opaque) => new()
    {
        Ping = new SessionResponse.Types.PingPong { Opaque = opaque }
    };

    public static SessionResponse Failure(StatusIds.Types.StatusCode code) => new()
    {
        Failure = new SessionResponse.Types.Failure { Status = code }
    };

    public static SessionResponse CreateSemaphoreResult(ulong reqId) => new()
    {
        CreateSemaphoreResult = new SessionResponse.Types.CreateSemaphoreResult { ReqId = reqId }
    };

    public static SessionResponse UpdateSemaphoreResult(ulong reqId) => new()
    {
        UpdateSemaphoreResult = new SessionResponse.Types.UpdateSemaphoreResult { ReqId = reqId }
    };

    public static SessionResponse DeleteSemaphoreResult(ulong reqId) => new()
    {
        DeleteSemaphoreResult = new SessionResponse.Types.DeleteSemaphoreResult { ReqId = reqId }
    };

    public static SessionResponse ReleaseSemaphoreResult(ulong reqId, bool released = true) => new()
    {
        ReleaseSemaphoreResult = new SessionResponse.Types.ReleaseSemaphoreResult
        {
            ReqId = reqId,
            Released = released
        }
    };

    public static SessionResponse AcquireSemaphoreResult(ulong reqId, bool acquired) => new()
    {
        AcquireSemaphoreResult = new SessionResponse.Types.AcquireSemaphoreResult
        {
            ReqId = reqId,
            Acquired = acquired
        }
    };

    public static SessionResponse AcquireSemaphorePending(ulong reqId) => new()
    {
        AcquireSemaphorePending = new SessionResponse.Types.AcquireSemaphorePending { ReqId = reqId }
    };

    public static SessionResponse DescribeSemaphoreResult(
        ulong reqId,
        string name,
        byte[]? data = null,
        bool watchAdded = false,
        ulong limit = 10,
        bool ephemeral = false,
        IEnumerable<SemaphoreSession>? owners = null,
        IEnumerable<SemaphoreSession>? waiters = null)
    {
        var description = new Ydb.Coordination.SemaphoreDescription
        {
            Name = name,
            Data = data is null ? ByteString.Empty : ByteString.CopyFrom(data),
            Count = (ulong)(owners?.Sum(o => (long)o.Count) ?? 0),
            Limit = limit,
            Ephemeral = ephemeral
        };

        if (owners is not null) description.Owners.Add(owners);
        if (waiters is not null) description.Waiters.Add(waiters);

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

    public static SessionResponse DescribeSemaphoreChanged(ulong reqId, bool dataChanged, bool ownersChanged) => new()
    {
        DescribeSemaphoreChanged = new SessionResponse.Types.DescribeSemaphoreChanged
        {
            ReqId = reqId,
            DataChanged = dataChanged,
            OwnersChanged = ownersChanged
        }
    };

    public static SemaphoreSession Owner(ulong sessionId, byte[]? data = null, ulong count = 1, ulong orderId = 0)
        => new()
        {
            SessionId = sessionId,
            Count = count,
            Data = data is null ? ByteString.Empty : ByteString.CopyFrom(data),
            OrderId = orderId
        };

    private sealed class StreamState
    {
        public ConcurrentQueue<SessionRequest> Written { get; } = new();
        public Channel<SessionResponse> Responses { get; } = Channel.CreateUnbounded<SessionResponse>(
            new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });
        public SessionResponse Current { get; set; } = new();
    }
}
