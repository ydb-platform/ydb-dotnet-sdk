using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading.Channels;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Internal;
using Ydb.Sdk.Coordination.Settings;
using SemaphoreDescription = Ydb.Sdk.Coordination.Description.SemaphoreDescription;

namespace Ydb.Sdk.Coordination;

/// <summary>
/// A long-lived attachment to a YDB coordination node.
/// </summary>
/// <remarks>
/// <para>
/// The session owns one bidirectional gRPC stream (<c>CoordinationService.Session</c>) for its entire
/// lifetime. A single internal worker task is the sole writer to that stream — every public method
/// enqueues its request into a channel and the worker writes them one at a time, eliminating any
/// race on the gRPC stream.
/// </para>
/// <para>
/// On a transport drop the worker transparently reconnects, replays pinned requests (waiters for
/// <c>AcquireSemaphore</c>) and re-arms watchers. Non-pinned in-flight requests are completed with
/// <see cref="Internal.SessionReconnectException"/> so the public helpers re-issue them with a fresh
/// <c>reqId</c>.
/// </para>
/// <para>
/// When the server reports a non-recoverable failure (<c>BadSession</c>, <c>SessionExpired</c>) the
/// session permanently dies; <see cref="SessionLostToken"/> is cancelled and every subsequent call
/// throws <see cref="YdbException"/> with <see cref="StatusCode.SessionExpired"/>.
/// </para>
/// </remarks>
public sealed class CoordinationSession : IAsyncDisposable
{
    private readonly IDriver _driver;
    private readonly string _nodePath;
    private readonly CoordinationSessionOptions _options;
    private readonly ILogger<CoordinationSession> _logger;

    private readonly Channel<SessionRequest> _outgoing = Channel.CreateUnbounded<SessionRequest>(
        new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private readonly ConcurrentDictionary<ulong, PendingRequest> _pending = new();
    private readonly WatcherRegistry _watchers = new();

    private readonly CancellationTokenSource _disposeCts = new();
    private readonly CancellationTokenSource _sessionLostCts = new();

    private readonly TaskCompletionSource _firstAttached =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly Task _workerTask;

    private long _reqIdCounter;
    private long _seqNo;

    private volatile bool _disposed;
    private volatile bool _sessionLost;

    // Mutable session attachment state — touched only on the worker thread.
    private ulong _sessionId;
    private byte[] _protectionKey = [];

    // Signals "stream is currently attached and ready to accept writes".
    // Replaced (CAS) by the worker on every reconnect.
    private volatile TaskCompletionSource _streamReady =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal CoordinationSession(IDriver driver, string nodePath, CoordinationSessionOptions? options = null)
    {
        _driver = driver ?? throw new ArgumentNullException(nameof(driver));
        _nodePath = nodePath ?? throw new ArgumentNullException(nameof(nodePath));
        _options = options ?? CoordinationSessionOptions.Default;
        _logger = driver.LoggerFactory.CreateLogger<CoordinationSession>();
        _workerTask = Task.Run(WorkerLoopAsync);
    }

    /// <summary>
    /// Server-assigned session identifier. Returns 0 until the first <c>SessionStarted</c> reply.
    /// </summary>
    public ulong SessionId => _sessionId;

    /// <summary>
    /// The path of the coordination node this session is attached to.
    /// </summary>
    public string NodePath => _nodePath;

    /// <summary>
    /// Cancelled when the session is irrecoverably lost (server expired it or the retry policy
    /// exhausted its attempts) or after <see cref="DisposeAsync"/>.
    /// </summary>
    public CancellationToken SessionLostToken => _sessionLostCts.Token;

    /// <summary>
    /// Awaits the first successful <c>SessionStarted</c> reply from the server.
    /// </summary>
    public Task WaitReadyAsync(CancellationToken cancellationToken = default)
        => _firstAttached.Task.WaitAsync(cancellationToken);

    // ------------------------------------------------------------------------------------------
    // Public API — semaphore management
    // ------------------------------------------------------------------------------------------

    public async Task CreateSemaphoreAsync(string name, ulong limit, byte[]? data = null,
        CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(reqId => new SessionRequest
        {
            CreateSemaphore = new SessionRequest.Types.CreateSemaphore
            {
                Name = name,
                Limit = limit,
                Data = ToByteString(data),
                ReqId = reqId
            }
        }, isPinned: false, cancellationToken).ConfigureAwait(false);

        EnsureResponseCase(response, SessionResponse.ResponseOneofCase.CreateSemaphoreResult, name);
    }

    public async Task UpdateSemaphoreAsync(string name, byte[]? data,
        CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(reqId => new SessionRequest
        {
            UpdateSemaphore = new SessionRequest.Types.UpdateSemaphore
            {
                Name = name,
                Data = ToByteString(data),
                ReqId = reqId
            }
        }, isPinned: false, cancellationToken).ConfigureAwait(false);

        EnsureResponseCase(response, SessionResponse.ResponseOneofCase.UpdateSemaphoreResult, name);
    }

    public async Task DeleteSemaphoreAsync(string name, bool force = false,
        CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(reqId => new SessionRequest
        {
            DeleteSemaphore = new SessionRequest.Types.DeleteSemaphore
            {
                Name = name,
                Force = force,
                ReqId = reqId
            }
        }, isPinned: false, cancellationToken).ConfigureAwait(false);

        EnsureResponseCase(response, SessionResponse.ResponseOneofCase.DeleteSemaphoreResult, name);
    }

    public async Task<SemaphoreDescription> DescribeSemaphoreAsync(
        string name,
        DescribeSemaphoreMode mode = DescribeSemaphoreMode.DataOnly,
        CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(reqId => new SessionRequest
        {
            DescribeSemaphore = new SessionRequest.Types.DescribeSemaphore
            {
                Name = name,
                IncludeOwners = mode.IncludeOwners(),
                IncludeWaiters = mode.IncludeWaiters(),
                ReqId = reqId
            }
        }, isPinned: false, cancellationToken).ConfigureAwait(false);

        EnsureResponseCase(response, SessionResponse.ResponseOneofCase.DescribeSemaphoreResult, name);
        return SemaphoreDescription.FromProto(response.DescribeSemaphoreResult.SemaphoreDescription);
    }

    /// <summary>
    /// Acquires a semaphore, waiting up to <paramref name="timeout"/> for it to become available.
    /// </summary>
    /// <param name="name">Semaphore name.</param>
    /// <param name="count">Acquire count. Use <see cref="ulong.MaxValue"/> for exclusive lock.</param>
    /// <param name="ephemeral">If <c>true</c>, the semaphore is created on-demand and removed once released.</param>
    /// <param name="data">Optional payload attached to the owner record (visible to other participants).</param>
    /// <param name="timeout">Maximum wait time. <c>null</c> ⇒ wait indefinitely; <see cref="TimeSpan.Zero"/> ⇒ try-acquire.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A <see cref="Lease"/> handle, or <c>null</c> if the wait timed out / try-acquire failed.</returns>
    public async Task<Lease?> AcquireSemaphoreAsync(
        string name,
        ulong count = 1,
        bool ephemeral = false,
        byte[]? data = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var timeoutMillis = timeout is null ? ulong.MaxValue : (ulong)Math.Max(0, timeout.Value.TotalMilliseconds);

        SessionRequest BuildRequest(ulong reqId) => new()
        {
            AcquireSemaphore = new SessionRequest.Types.AcquireSemaphore
            {
                Name = name,
                Count = count,
                Data = ToByteString(data),
                Ephemeral = ephemeral,
                TimeoutMillis = timeoutMillis,
                ReqId = reqId
            }
        };

        var response = await SendAsync(BuildRequest, isPinned: true, cancellationToken).ConfigureAwait(false);

        EnsureResponseCase(response, SessionResponse.ResponseOneofCase.AcquireSemaphoreResult, name);

        if (!response.AcquireSemaphoreResult.Acquired)
            return null;

        return new Lease(this, name);
    }

    public async Task ReleaseSemaphoreAsync(string name, CancellationToken cancellationToken = default)
    {
        var response = await SendAsync(reqId => new SessionRequest
        {
            ReleaseSemaphore = new SessionRequest.Types.ReleaseSemaphore
            {
                Name = name,
                ReqId = reqId
            }
        }, isPinned: false, cancellationToken).ConfigureAwait(false);

        EnsureResponseCase(response, SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult, name);
    }

    /// <summary>
    /// Subscribes to changes of a semaphore. The returned <see cref="WatchResult{T}"/> exposes the initial
    /// snapshot and an <see cref="IAsyncEnumerable{T}"/> of subsequent descriptions, re-fetched whenever
    /// the server reports a change.
    /// </summary>
    public async Task<WatchResult<SemaphoreDescription>> WatchSemaphoreAsync(
        string name,
        DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode,
        CancellationToken cancellationToken = default)
    {
        var subscription = _watchers.Watch(name);

        SemaphoreDescription initial;
        try
        {
            initial = await DescribeWithWatchAsync(name, describeMode, watchMode, subscription, cancellationToken)
                .ConfigureAwait(false);
        }
        catch
        {
            _watchers.Remove(subscription);
            throw;
        }

        return new WatchResult<SemaphoreDescription>(initial, IterateAsync());

        async IAsyncEnumerable<SemaphoreDescription> IterateAsync(
            [EnumeratorCancellation] CancellationToken ct = default)
        {
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, ct);
            try
            {
                await foreach (var _ in subscription.ReadAllAsync(linked.Token).ConfigureAwait(false))
                {
                    yield return await DescribeWithWatchAsync(
                        name, describeMode, watchMode, subscription, linked.Token).ConfigureAwait(false);
                }
            }
            finally
            {
                _watchers.Remove(subscription);
            }
        }
    }

    // ------------------------------------------------------------------------------------------
    // Internal: dispatch
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Send-and-await helper. Allocates a fresh reqId, registers a pending TCS, enqueues the request
    /// onto the writer channel and waits for the matching response. Retries automatically on a
    /// transparent reconnect (signaled by <see cref="SessionReconnectException"/>) unless the request
    /// is pinned (in which case the worker resends it with the same reqId).
    /// </summary>
    internal async Task<SessionResponse> SendAsync(
        Func<ulong, SessionRequest> factory,
        bool isPinned,
        CancellationToken cancellationToken)
    {
        while (true)
        {
            ThrowIfShutdown();
            cancellationToken.ThrowIfCancellationRequested();

            await WaitStreamReadyAsync(cancellationToken).ConfigureAwait(false);

            var reqId = NextReqId();
            var request = factory(reqId);
            var pending = new PendingRequest(reqId, request, isPinned);

            if (!_pending.TryAdd(reqId, pending))
                throw new InvalidOperationException($"Duplicate reqId {reqId} (bug in CoordinationSession)");

            try
            {
                if (!_outgoing.Writer.TryWrite(request))
                {
                    _pending.TryRemove(reqId, out _);
                    ThrowIfShutdown();
                    continue;
                }

                await using var ctsRegistration = cancellationToken.Register(() =>
                {
                    if (_pending.TryRemove(reqId, out var p))
                        p.Tcs.TrySetCanceled(cancellationToken);
                }).ConfigureAwait(false);

                return await pending.Tcs.Task.ConfigureAwait(false);
            }
            catch (SessionReconnectException)
            {
                if (isPinned)
                {
                    // The worker has already re-sent us with the same reqId — we should be re-queued.
                    // Fall through and retry transparently.
                    continue;
                }

                continue;
            }
            catch
            {
                _pending.TryRemove(reqId, out _);
                throw;
            }
        }
    }

    private async Task WaitStreamReadyAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            ThrowIfShutdown();

            var tcs = _streamReady;
            if (tcs.Task.IsCompletedSuccessfully)
                return;

            await tcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private ulong NextReqId() => (ulong)Interlocked.Increment(ref _reqIdCounter);

    private void ThrowIfShutdown()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(CoordinationSession));
        if (_sessionLost)
            throw new YdbException(StatusCode.SessionExpired,
                $"Coordination session for '{_nodePath}' has been lost");
    }

    private async Task<SemaphoreDescription> DescribeWithWatchAsync(
        string name,
        DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode,
        WatchSubscription subscription,
        CancellationToken cancellationToken)
    {
        ulong reqId = 0;

        var response = await SendAsync(id =>
        {
            reqId = id;
            return new SessionRequest
            {
                DescribeSemaphore = new SessionRequest.Types.DescribeSemaphore
                {
                    Name = name,
                    IncludeOwners = describeMode.IncludeOwners(),
                    IncludeWaiters = describeMode.IncludeWaiters(),
                    WatchData = watchMode.WatchData(),
                    WatchOwners = watchMode.WatchOwners(),
                    ReqId = id
                }
            };
        }, isPinned: false, cancellationToken).ConfigureAwait(false);

        EnsureResponseCase(response, SessionResponse.ResponseOneofCase.DescribeSemaphoreResult, name);

        if (response.DescribeSemaphoreResult.WatchAdded)
            _watchers.Bind(subscription, reqId);

        return SemaphoreDescription.FromProto(response.DescribeSemaphoreResult.SemaphoreDescription);
    }

    // ------------------------------------------------------------------------------------------
    // Worker loop
    // ------------------------------------------------------------------------------------------

    private async Task WorkerLoopAsync()
    {
        var attempt = 0;
        while (!_disposeCts.IsCancellationRequested && !_sessionLost)
        {
            try
            {
                var gracefulStop = await RunOneStreamAsync().ConfigureAwait(false);
                if (gracefulStop)
                    break;
                attempt = 0;
            }
            catch (OperationCanceledException) when (_disposeCts.IsCancellationRequested)
            {
                break;
            }
            catch (YdbException ex)
            {
                var delay = _options.RetryPolicy.GetNextDelay(ex, attempt++);
                if (delay is null)
                {
                    _logger.LogError(ex, "Coordination session worker giving up — non-retryable error");
                    break;
                }

                _logger.LogWarning(ex,
                    "Coordination session stream broken; reconnect attempt {Attempt} in {Delay}",
                    attempt, delay.Value);

                ResetStreamReady();
                FailNonPinnedPending();

                try
                {
                    await Task.Delay(delay.Value, _disposeCts.Token).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    break;
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Coordination session worker terminated unexpectedly");
                break;
            }
        }

        ShutdownCore();
    }

    /// <returns><c>true</c> if the session was gracefully stopped (no more reconnects),
    /// <c>false</c> if the stream failed and the worker should reconnect.</returns>
    private async Task<bool> RunOneStreamAsync()
    {
        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(_disposeCts.Token);

        var sessionStartedTcs = new TaskCompletionSource<ulong>(TaskCreationOptions.RunContinuationsAsynchronously);
        var sessionStoppedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        IBidirectionalStream<SessionRequest, SessionResponse>? stream = null;
        Task? reader = null;
        try
        {
            stream = await _driver
                .BidirectionalStreamCall(CoordinationService.SessionMethod, new GrpcRequestSettings())
                .ConfigureAwait(false);

            reader = ReaderLoopAsync(stream, sessionStartedTcs, sessionStoppedTcs, streamCts);

            await AttachSessionAsync(stream, sessionStartedTcs, streamCts.Token).ConfigureAwait(false);

            await ReplayPinnedAsync(stream, streamCts.Token).ConfigureAwait(false);

            _watchers.NotifyAllWatches();

            _streamReady.TrySetResult();
            _firstAttached.TrySetResult();

            try
            {
                await PumpOutgoingAsync(stream, streamCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (!_disposeCts.IsCancellationRequested)
            {
                // The stream was broken by the reader. Fall through and let the post-loop logic
                // decide whether this was a graceful stop or a transport drop.
            }

            if (_disposeCts.IsCancellationRequested)
                return true;

            if (sessionStoppedTcs.Task.IsCompletedSuccessfully)
            {
                _logger.LogInformation("Coordination session {SessionId} stopped gracefully", _sessionId);
                return true;
            }

            if (_sessionLost)
                return true;

            throw new YdbException(StatusCode.Unavailable, "Coordination stream closed unexpectedly");
        }
        finally
        {
            streamCts.Cancel();
            if (reader is not null)
            {
                try { await reader.ConfigureAwait(false); }
                catch { /* logged in reader */ }
            }

            stream?.Dispose();
        }
    }

    private async Task AttachSessionAsync(
        IBidirectionalStream<SessionRequest, SessionResponse> stream,
        TaskCompletionSource<ulong> sessionStartedTcs,
        CancellationToken cancellationToken)
    {
        var isResume = _sessionId != 0;
        if (!isResume)
        {
            _protectionKey = CreateProtectionKey();
            Interlocked.Exchange(ref _seqNo, 0);
        }

        var seqNo = (ulong)Interlocked.Increment(ref _seqNo);

        var sessionStart = new SessionRequest
        {
            SessionStart = new SessionRequest.Types.SessionStart
            {
                SessionId = _sessionId,
                Path = _nodePath,
                Description = _options.Description,
                TimeoutMillis = (ulong)_options.ConnectTimeout.TotalMilliseconds,
                ProtectionKey = ByteString.CopyFrom(_protectionKey),
                SeqNo = seqNo
            }
        };

        await stream.Write(sessionStart).WaitAsync(_options.ConnectTimeout, cancellationToken).ConfigureAwait(false);

        var newSessionId = await sessionStartedTcs.Task
            .WaitAsync(_options.ConnectTimeout, cancellationToken)
            .ConfigureAwait(false);

        _sessionId = newSessionId;
    }

    private async Task ReplayPinnedAsync(
        IBidirectionalStream<SessionRequest, SessionResponse> stream,
        CancellationToken cancellationToken)
    {
        foreach (var pending in _pending.Values.Where(p => p.IsPinned))
        {
            cancellationToken.ThrowIfCancellationRequested();
            await stream.Write(pending.Request).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task PumpOutgoingAsync(
        IBidirectionalStream<SessionRequest, SessionResponse> stream,
        CancellationToken cancellationToken)
    {
        await foreach (var request in _outgoing.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            if (!IsRequestStillRelevant(request))
                continue;

            await stream.Write(request).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private bool IsRequestStillRelevant(SessionRequest request)
    {
        if (TryExtractReqId(request, out var reqId))
            return _pending.ContainsKey(reqId);

        // Infrastructure messages (SessionStart/Stop, Pong) have no reqId — always send.
        return true;
    }

    private async Task ReaderLoopAsync(
        IBidirectionalStream<SessionRequest, SessionResponse> stream,
        TaskCompletionSource<ulong> sessionStartedTcs,
        TaskCompletionSource sessionStoppedTcs,
        CancellationTokenSource streamCts)
    {
        try
        {
            while (await stream.MoveNextAsync().ConfigureAwait(false))
            {
                var response = stream.Current;

                switch (response.ResponseCase)
                {
                    case SessionResponse.ResponseOneofCase.SessionStarted:
                        sessionStartedTcs.TrySetResult(response.SessionStarted.SessionId);
                        continue;

                    case SessionResponse.ResponseOneofCase.SessionStopped:
                        sessionStoppedTcs.TrySetResult();
                        return;

                    case SessionResponse.ResponseOneofCase.Ping:
                        _outgoing.Writer.TryWrite(new SessionRequest
                        {
                            Pong = new SessionRequest.Types.PingPong { Opaque = response.Ping.Opaque }
                        });
                        continue;

                    case SessionResponse.ResponseOneofCase.Failure:
                        HandleFailure(response.Failure);
                        return;

                    case SessionResponse.ResponseOneofCase.DescribeSemaphoreChanged:
                        _watchers.Notify(response.DescribeSemaphoreChanged);
                        continue;

                    case SessionResponse.ResponseOneofCase.AcquireSemaphorePending:
                        // Interim notification — the waiter is queued on the server. Keep the
                        // pending entry so the eventual AcquireSemaphoreResult resolves it.
                        continue;
                }

                if (TryExtractResponseReqId(response, out var reqId) &&
                    _pending.TryRemove(reqId, out var pending))
                {
                    pending.Tcs.TrySetResult(response);
                }
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Coordination session reader loop failed");
            sessionStartedTcs.TrySetException(ex);
        }
        finally
        {
            streamCts.Cancel();
        }
    }

    private void HandleFailure(SessionResponse.Types.Failure failure)
    {
        _logger.LogWarning(
            "Coordination session received Failure: status={Status} issues={Issues}",
            failure.Status, failure.Issues);

        _sessionLost = true;
        _sessionLostCts.Cancel();
    }

    private void FailNonPinnedPending()
    {
        foreach (var (reqId, pending) in _pending.ToArray())
        {
            if (pending.IsPinned)
                continue;
            if (_pending.TryRemove(reqId, out _))
                pending.Tcs.TrySetException(new SessionReconnectException());
        }
    }

    private void ResetStreamReady()
    {
        var fresh = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Interlocked.Exchange(ref _streamReady, fresh);
    }

    private void ShutdownCore()
    {
        if (!_sessionLostCts.IsCancellationRequested)
            _sessionLostCts.Cancel();

        _firstAttached.TrySetException(new YdbException(StatusCode.SessionExpired,
            $"Coordination session for '{_nodePath}' terminated before attaching"));

        foreach (var (reqId, pending) in _pending.ToArray())
        {
            if (_pending.TryRemove(reqId, out _))
                pending.Tcs.TrySetException(new YdbException(StatusCode.SessionExpired,
                    "Coordination session terminated"));
        }

        _streamReady.TrySetException(new YdbException(StatusCode.SessionExpired,
            "Coordination session terminated"));

        _watchers.Dispose();
    }

    // ------------------------------------------------------------------------------------------
    // Dispose
    // ------------------------------------------------------------------------------------------

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        _disposed = true;

        try
        {
            if (!_sessionLost && _outgoing.Writer.TryWrite(new SessionRequest
            {
                SessionStop = new SessionRequest.Types.SessionStop()
            }))
            {
                try
                {
                    await _workerTask.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);
                }
                catch (TimeoutException)
                {
                }
                catch
                {
                    // ignored — worker logs its own errors
                }
            }
        }
        catch
        {
            // ignore — we're tearing down anyway
        }
        finally
        {
            _outgoing.Writer.TryComplete();
            await _disposeCts.CancelAsync().ConfigureAwait(false);

            try { await _workerTask.ConfigureAwait(false); }
            catch { /* worker logged */ }

            _disposeCts.Dispose();
            _sessionLostCts.Dispose();

            GC.SuppressFinalize(this);
        }
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private static ByteString ToByteString(byte[]? data) =>
        data is null ? ByteString.Empty : ByteString.CopyFrom(data);

    private static byte[] CreateProtectionKey()
    {
        var key = new byte[16];
        RandomNumberGenerator.Fill(key);
        return key;
    }

    private static bool TryExtractReqId(SessionRequest request, out ulong reqId)
    {
        switch (request.RequestCase)
        {
            case SessionRequest.RequestOneofCase.AcquireSemaphore:
                reqId = request.AcquireSemaphore.ReqId;
                return true;
            case SessionRequest.RequestOneofCase.ReleaseSemaphore:
                reqId = request.ReleaseSemaphore.ReqId;
                return true;
            case SessionRequest.RequestOneofCase.DescribeSemaphore:
                reqId = request.DescribeSemaphore.ReqId;
                return true;
            case SessionRequest.RequestOneofCase.CreateSemaphore:
                reqId = request.CreateSemaphore.ReqId;
                return true;
            case SessionRequest.RequestOneofCase.UpdateSemaphore:
                reqId = request.UpdateSemaphore.ReqId;
                return true;
            case SessionRequest.RequestOneofCase.DeleteSemaphore:
                reqId = request.DeleteSemaphore.ReqId;
                return true;
            default:
                reqId = 0;
                return false;
        }
    }

    private static bool TryExtractResponseReqId(SessionResponse response, out ulong reqId)
    {
        switch (response.ResponseCase)
        {
            case SessionResponse.ResponseOneofCase.AcquireSemaphorePending:
                reqId = response.AcquireSemaphorePending.ReqId;
                return true;
            case SessionResponse.ResponseOneofCase.AcquireSemaphoreResult:
                reqId = response.AcquireSemaphoreResult.ReqId;
                return true;
            case SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult:
                reqId = response.ReleaseSemaphoreResult.ReqId;
                return true;
            case SessionResponse.ResponseOneofCase.DescribeSemaphoreResult:
                reqId = response.DescribeSemaphoreResult.ReqId;
                return true;
            case SessionResponse.ResponseOneofCase.CreateSemaphoreResult:
                reqId = response.CreateSemaphoreResult.ReqId;
                return true;
            case SessionResponse.ResponseOneofCase.UpdateSemaphoreResult:
                reqId = response.UpdateSemaphoreResult.ReqId;
                return true;
            case SessionResponse.ResponseOneofCase.DeleteSemaphoreResult:
                reqId = response.DeleteSemaphoreResult.ReqId;
                return true;
            default:
                reqId = 0;
                return false;
        }
    }

    private static void EnsureResponseCase(
        SessionResponse response,
        SessionResponse.ResponseOneofCase expected,
        string context)
    {
        if (response.ResponseCase != expected)
            throw new YdbException(StatusCode.InternalError,
                $"Unexpected response case {response.ResponseCase} (expected {expected}) for '{context}'");
    }
}
