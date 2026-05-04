using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.RequestRegistry;
using Ydb.Sdk.Coordination.Settings;
using Ydb.Sdk.Coordination.Watcher;
using SemaphoreDescription = Ydb.Sdk.Coordination.Description.SemaphoreDescription;

namespace Ydb.Sdk.Coordination;

public class SessionTransport : IAsyncDisposable
{
    private readonly SessionOptions _sessionOptions;
    private readonly IDriver _driver;

    private readonly CancellationTokenSource _cancelTokenSource;

    public ulong SessionId { get; private set; }
    private ulong _seqNo;
    private readonly string _pathNode;

    private TaskCompletionSource? _sessionStartedTcs;

    // Bidirectional stream handler
    private IBidirectionalStream<SessionRequest, SessionResponse>? _stream;
    private readonly YdbRetryPolicyExecutor _executor;
    private readonly SessionRequestRegistry _requestRegistry = new();
    public StateSession StateSession { get; private set; } = StateSession.Initial;
    private byte[] _key = [];

    private readonly TaskCompletionSource _firstSessionStartedTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly Task _initTask;

    private volatile TaskCompletionSource _sessionReadyTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private volatile bool _disposed;
    private volatile bool _streamClosed;


    private readonly WatcherRegistry _watcherRegistry = new();

    private readonly ILogger<SessionTransport> _logger;

    private volatile bool _sessionStopped;
    private readonly CancellationTokenSource _sessionStoppedCts = new();

    public SessionTransport(IDriver driver, string pathNode, SessionOptions sessionOptions,
        CancellationTokenSource? cancelTokenSource)
    {
        _logger = driver.LoggerFactory.CreateLogger<SessionTransport>();
        _sessionOptions = sessionOptions;
        _logger.LogInformation("Creating session on {Path}", pathNode);
        _executor = new YdbRetryPolicyExecutor(_sessionOptions.RetryPolicy);
        _driver = driver;
        _pathNode = pathNode;
        _cancelTokenSource = cancelTokenSource ?? new CancellationTokenSource();
        _initTask = RecoverSession();
    }

    private async Task RecoverSession()
    {
        await _executor.ExecuteAsync(async ct =>
        {
            switch (StateSession)
            {
                case StateSession.Initial:
                    await CreateSession(false, ct);
                    StateSession = StateSession.Connected;
                    break;
                case StateSession.Recovery:
                    try
                    {
                        await CreateSession(true, ct);
                        StateSession = StateSession.Connected;
                    }
                    catch (YdbException)
                    {
                        StateSession = StateSession.Reconnecting;
                        goto case StateSession.Reconnecting;
                    }

                    break;
                case StateSession.Reconnecting:
                    await CreateSession(false, ct);
                    StateSession = StateSession.Connected;
                    break;
            }

            /*
            if (sessionRecovery)
            {
                await CreateSession(true, ct);
            }
            else
            {
                await CreateSession(false, ct);
            }
            */
        }, _cancelTokenSource.Token);
    }

    private async Task CloseBrokenStream()
    {
        var stream = _stream;
        _stream = null;
        _streamClosed = true;

        if (stream == null)
            return;

        try
        {
            _requestRegistry.Reconnect();
            await stream.RequestStreamComplete().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Error while completing broken request stream");
        }
        finally
        {
            stream.Dispose();
        }
    }

    private async Task OpenStream(bool isSessionRecovery, CancellationToken cancellationToken = default)
    {
        StateSession = StateSession.Connecting;
        _stream = await _driver.BidirectionalStreamCall(CoordinationService.SessionMethod, new GrpcRequestSettings());
        if (_stream == null)
            throw new InvalidOperationException("Stream is null in SendStartSession");
        _ = Task.Run(RunProcessingStreamResponse, cancellationToken);
        await SendStartSession(isSessionRecovery);
        _watcherRegistry.NotifyAllWatches();
        StateSession = StateSession.Connected;
    }

    private async Task CreateSession(bool isSessionRecovery, CancellationToken cancellationToken = default)
    {
        var newTcs = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously);
        Interlocked.Exchange(ref _sessionReadyTcs, newTcs);
        try
        {
            await CloseBrokenStream();
            await OpenStream(isSessionRecovery, cancellationToken);

            newTcs.TrySetResult();
        }
        catch (YdbException ex)
        {
            newTcs.TrySetException(ex);
            throw;
        }
    }

    private async Task RunProcessingStreamResponse()
    {
        try
        {
            while (await _stream!.MoveNextAsync())
            {
                var response = _stream.Current;
                switch (response.ResponseCase)
                {
                    case SessionResponse.ResponseOneofCase.Ping:
                        await HandlePing(response);
                        break;
                    case SessionResponse.ResponseOneofCase.Failure:
                        await HandleFailure(response);
                        break;
                    case SessionResponse.ResponseOneofCase.SessionStarted:
                        HandleSessionStarted(response);
                        break;
                    case SessionResponse.ResponseOneofCase.SessionStopped:
                        HandleSessionStopped(response);
                        break;
                    case SessionResponse.ResponseOneofCase.DescribeSemaphoreChanged:
                        HandleSemaphoreChanged(response.DescribeSemaphoreChanged);
                        break;
                    case SessionResponse.ResponseOneofCase.None:
                    case SessionResponse.ResponseOneofCase.Pong:
                    case SessionResponse.ResponseOneofCase.Unsupported6:
                    case SessionResponse.ResponseOneofCase.Unsupported7:
                    case SessionResponse.ResponseOneofCase.AcquireSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.DescribeSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.CreateSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.UpdateSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.DeleteSemaphoreResult:
                    case SessionResponse.ResponseOneofCase.Unsupported16:
                    case SessionResponse.ResponseOneofCase.Unsupported17:
                    case SessionResponse.ResponseOneofCase.Unsupported18:
                        break;
                }

                var reqId = ExtractReqId(response);
                if (reqId.HasValue)
                {
                    _requestRegistry.Resolve(reqId.Value, response);
                }
            }
        }
        catch (YdbException ex)
        {
            _logger.LogError(ex, "Stream processing failed");
            StateSession = StateSession.Recovery;
            throw;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Stream processing failed");
            StateSession = StateSession.Recovery;
            throw new YdbException(StatusCode.BadSession, ex.Message);
        }
    }


    public CancellationToken Token => _cancelTokenSource.Token;

    public async Task CreateSemaphore(string name, ulong limit, byte[]? data,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating {Name} (limit={Limit})", name, limit);
        var combineToken = LinkToken(cancellationToken);

        try
        {
            var response = await SendRequest(reqId => new SessionRequest
            {
                CreateSemaphore = new SessionRequest.Types.CreateSemaphore
                {
                    Name = name,
                    Limit = limit,
                    Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                    ReqId = reqId
                }
            }, combineToken);

            if (response.ResponseCase != SessionResponse.ResponseOneofCase.CreateSemaphoreResult)
            {
                throw new YdbException("Unexpected response for createSemaphore");
            }

            Status.FromProto(response.CreateSemaphoreResult.Status, response.CreateSemaphoreResult.Issues)
                .EnsureSuccess();
        }
        catch (Exception e)
        {
            throw new YdbException("Create semaphore failed " + e.Message);
        }
    }


    public async Task UpdateSemaphore(string name, byte[]? data, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Updating data on {Name} ({Bytes} bytes)",
            name,
            data?.Length ?? 0
        );
        var combineToken = LinkToken(cancellationToken);
        try
        {
            var response = await SendRequest(reqId => new SessionRequest
            {
                UpdateSemaphore = new SessionRequest.Types.UpdateSemaphore
                {
                    Name = name,
                    Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                    ReqId = reqId
                }
            }, combineToken);
            if (response.ResponseCase != SessionResponse.ResponseOneofCase.UpdateSemaphoreResult)
            {
                throw new YdbException("Unexpected response for updateSemaphore");
            }

            Status.FromProto(response.UpdateSemaphoreResult.Status, response.UpdateSemaphoreResult.Issues)
                .EnsureSuccess();
        }
        catch (Exception e)
        {
            throw new YdbException("Update semaphore failed " + e.Message);
        }
    }


    public async Task DeleteSemaphore(string name, bool force, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Deleting {Name}, force = {Force}",
            name,
            force
        );
        var combineToken = LinkToken(cancellationToken);
        try
        {
            var response = await SendRequest(reqId => new SessionRequest
            {
                DeleteSemaphore = new SessionRequest.Types.DeleteSemaphore
                {
                    Name = name,
                    Force = force,
                    ReqId = reqId
                }
            }, combineToken);
            if (response.ResponseCase != SessionResponse.ResponseOneofCase.DeleteSemaphoreResult)
            {
                throw new YdbException("Unexpected response for deleteSemaphore");
            }

            Status.FromProto(response.DeleteSemaphoreResult.Status, response.DeleteSemaphoreResult.Issues)
                .EnsureSuccess();
        }
        catch (Exception e)
        {
            throw new YdbException("Delete semaphore failed " + e.Message);
        }
    }


    public async Task<SemaphoreDescription> DescribeSemaphore(string name,
        DescribeSemaphoreMode mode, CancellationToken cancellationToken = default)
    {
        var combineToken = LinkToken(cancellationToken);

        try
        {
            var response = await SendRequest(reqId => new SessionRequest
            {
                DescribeSemaphore = new SessionRequest.Types.DescribeSemaphore
                {
                    Name = name,
                    IncludeOwners = mode.IncludeOwners(),
                    IncludeWaiters = mode.IncludeWaiters(),
                    WatchData = false,
                    WatchOwners = false,
                    ReqId = reqId
                }
            }, combineToken);
            if (response.ResponseCase != SessionResponse.ResponseOneofCase.DescribeSemaphoreResult)
            {
                throw new YdbException("Unexpected response for describeSemaphore");
            }

            Status.FromProto(response.DescribeSemaphoreResult.Status, response.DescribeSemaphoreResult.Issues)
                .EnsureSuccess();
            return SemaphoreDescription.FromProto(response.DescribeSemaphoreResult
                .SemaphoreDescription);
        }
        catch (Exception e)
        {
            throw new YdbException("Describe semaphore failed " + e.Message);
        }
    }

    public async Task<bool> AcquireSemaphore(string name, ulong count, bool isEphemeral, byte[]? data,
        TimeSpan? timeout, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Waiting to acquire {Name} (count={Count})", name, count);
        var combineToken = LinkToken(cancellationToken);


        SessionRequest BuildRequest(ulong reqId)
        {
            return new SessionRequest
            {
                AcquireSemaphore = new SessionRequest.Types.AcquireSemaphore
                {
                    Name = name,
                    Count = count,
                    Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                    Ephemeral = isEphemeral,
                    TimeoutMillis = timeout == null ? ulong.MaxValue : (ulong)timeout.Value.TotalMilliseconds,
                    ReqId = reqId
                }
            };
        }

        try
        {
            ulong pinnedReqId = 0;
            // The initial request allocates a reqId that is pinned for the entire
            // acquire flow — the server uses it to identify the waiter slot.
            var response = await SendRequest(reqId =>
            {
                pinnedReqId = reqId;
                return BuildRequest(reqId);
            }, combineToken);

            // The server may respond with acquireSemaphorePending before the final
            // result. We keep waiting with the same pinned reqId — on reconnect
            // the full request is re-sent because the server lost the waiter state.

            while (response.ResponseCase == SessionResponse.ResponseOneofCase.AcquireSemaphorePending)
            {
                response = await SendRequestPinned(
                    pinnedReqId,
                    () =>
                        BuildRequest(pinnedReqId), cancellationToken
                );
            }


            if (response.ResponseCase != SessionResponse.ResponseOneofCase.AcquireSemaphoreResult)
            {
                throw new YdbException("Unexpected response for acquireSemaphore");
            }

            Status.FromProto(response.AcquireSemaphoreResult.Status, response.AcquireSemaphoreResult.Issues)
                .EnsureSuccess();
            return response.AcquireSemaphoreResult.Acquired;
        }
        catch (Exception e)
        {
            throw new YdbException("Acquire semaphore failed " + e.Message);
        }
    }

    public async Task ReleaseSemaphore(string name, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Releasing {Name}", name);
        var combineToken = LinkToken(cancellationToken);
        try
        {
            var response = await SendRequest(reqId => new SessionRequest
            {
                ReleaseSemaphore = new SessionRequest.Types.ReleaseSemaphore
                {
                    Name = name,
                    ReqId = reqId
                }
            }, combineToken);
            if (response.ResponseCase != SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult)
            {
                throw new YdbException("Unexpected response for releaseSemaphore");
            }

            Status.FromProto(response.ReleaseSemaphoreResult.Status, response.ReleaseSemaphoreResult.Issues)
                .EnsureSuccess();
        }
        catch (Exception e)
        {
            throw new YdbException("Release semaphore failed " + e.Message);
        }
    }

    public async Task<WatchResult<SemaphoreDescription>> WatchSemaphore(
        string name,
        DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode,
        CancellationToken cancellationToken = default)
    {
        var combineToken = LinkToken(cancellationToken);
        var subscription = _watcherRegistry.Watch(name);

        var firstResponse = await DescribeSemaphoreInternal(
            name,
            describeMode,
            watchMode,
            subscription,
            combineToken);
        var initial = SemaphoreDescription.FromProto(
            firstResponse.DescribeSemaphoreResult.SemaphoreDescription);
        return new WatchResult<SemaphoreDescription>(initial, Updates(combineToken));

        async IAsyncEnumerable<SemaphoreDescription> Updates(
            [EnumeratorCancellation] CancellationToken token = default)
        {
            try
            {
                await foreach (var _ in subscription.ReadAllAsync(token))
                {
                    var response = await DescribeSemaphoreInternal(
                        name,
                        describeMode,
                        watchMode,
                        subscription,
                        token);

                    yield return SemaphoreDescription.FromProto(
                        response.DescribeSemaphoreResult.SemaphoreDescription);
                }
            }
            finally
            {
                _watcherRegistry.RemoveWatch(name, subscription);
            }
        }
    }

    private async Task<SessionResponse> DescribeSemaphoreInternal(
        string name,
        DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode,
        WatchSubscription subscription,
        CancellationToken token)
    {
        try
        {
            ulong watchReqId = 0;
            var response = await SendRequest(reqId =>
            {
                watchReqId = reqId;
                return new SessionRequest
                {
                    DescribeSemaphore = new SessionRequest.Types.DescribeSemaphore
                    {
                        Name = name,
                        IncludeOwners = describeMode.IncludeOwners(),
                        IncludeWaiters = describeMode.IncludeWaiters(),
                        WatchData = watchMode.WatchData(),
                        WatchOwners = watchMode.WatchOwners(),
                        ReqId = reqId
                    }
                };
            }, token);

            if (response.ResponseCase != SessionResponse.ResponseOneofCase.DescribeSemaphoreResult)
            {
                throw new YdbException("Unexpected response for describeSemaphore (watch)");
            }

            Status.FromProto(response.DescribeSemaphoreResult.Status, response.DescribeSemaphoreResult.Issues)
                .EnsureSuccess();
            if (response.DescribeSemaphoreResult.WatchAdded)
            {
                _watcherRegistry.RemapWatch(name, subscription, watchReqId);
            }

            return response;
        }
        catch (Exception e)
        {
            _watcherRegistry.RemoveWatch(name, subscription);
            throw new YdbException("Watch semaphore failed " + e.Message);
        }
    }

    private static ulong? ExtractReqId(SessionResponse response) =>
        response.ResponseCase switch
        {
            SessionResponse.ResponseOneofCase.AcquireSemaphorePending => response.AcquireSemaphorePending.ReqId,
            SessionResponse.ResponseOneofCase.AcquireSemaphoreResult => response.AcquireSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult => response.ReleaseSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.DescribeSemaphoreResult => response.DescribeSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.CreateSemaphoreResult => response.CreateSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.UpdateSemaphoreResult => response.UpdateSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.DeleteSemaphoreResult => response.DeleteSemaphoreResult.ReqId,
            _ => null
        };

    private CancellationToken LinkToken(CancellationToken token)
    {
        if (token == CancellationToken.None)
            return _cancelTokenSource.Token;

        return CancellationTokenSource
            .CreateLinkedTokenSource(_cancelTokenSource.Token, token)
            .Token;
    }

    private async Task SendStartSession(bool isSessionRecovery)
    {
        if (_stream == null)
            throw new InvalidOperationException("Stream not initialized");

        _sessionStartedTcs =
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (!isSessionRecovery)
        {
            SessionId = 0;
            _key = CreateRandomKey();
        }

        var initRequestStart = new SessionRequest
        {
            SessionStart = new SessionRequest.Types.SessionStart
            {
                SessionId = SessionId,
                Path = _pathNode,
                Description = _sessionOptions.Description,
                TimeoutMillis = (ulong)_sessionOptions.StartTimeout.TotalMilliseconds,
                ProtectionKey = ByteString.CopyFrom(_key),
                SeqNo = _seqNo++
            }
        };

        await SafeWrite(initRequestStart, _cancelTokenSource.Token);

        await _sessionStartedTcs.Task;
    }

    private async Task SendStop()
    {
        var stopSession = new SessionRequest
        {
            SessionStop = new SessionRequest.Types.SessionStop()
        };
        try
        {
            await SafeWrite(stopSession, _cancelTokenSource.Token);
        }
        catch (Exception)
        {
            throw new YdbException("Stop session failed");
        }
    }


    private async Task HandlePing(SessionResponse response)
    {
        var opaque = response.Ping.Opaque;
        var pongRequest = new SessionRequest
        {
            Pong = new SessionRequest.Types.PingPong
            {
                Opaque = opaque
            }
        };
        await SafeWrite(pongRequest, _cancelTokenSource.Token);
    }

    private async Task HandleFailure(SessionResponse response)
    {
        var failure = response.Failure;

        _logger.LogWarning(
            "Session failure received. Status: {Status}, Issues: {Issues}",
            failure.Status,
            failure.Issues);

        if (failure.Status == StatusIds.Types.StatusCode.BadSession ||
            failure.Status == StatusIds.Types.StatusCode.SessionExpired)
        {
            _logger.LogInformation("Session is invalid or expired");
            SessionId = 0;
        }

        await _cancelTokenSource.CancelAsync();
    }

    private void HandleSessionStarted(SessionResponse response)
    {
        _logger.LogTrace("Session started with id {SessionId}", response.SessionStarted.SessionId);
        SessionId = response.SessionStarted.SessionId;
        _sessionStartedTcs?.TrySetResult();
        _sessionStartedTcs = null;
        _firstSessionStartedTcs.TrySetResult();
    }

    private void HandleSessionStopped(SessionResponse response)
    {
        _logger.LogTrace("Session stopped with id {SessionId}", response.SessionStopped.SessionId);
        _sessionStopped = true;

        if (!_sessionStoppedCts.IsCancellationRequested)
            _sessionStoppedCts.Cancel();
    }

    private void HandleSemaphoreChanged(SessionResponse.Types.DescribeSemaphoreChanged change)
        => _watcherRegistry.Notify(change);


    private static byte[] CreateRandomKey()
    {
        var protectionKey = new byte[16];
        RandomNumberGenerator.Fill(protectionKey);
        return protectionKey;
    }

    private async Task SafeWrite(SessionRequest request, CancellationToken cancellationToken = default)
    {
        await _writeLock.WaitAsync(cancellationToken);
        try
        {
            await _stream!.Write(request).WaitAsync(cancellationToken);
        }
        finally
        {
            _writeLock.Release();
        }
    }

    private void FailSession(Exception ex)
    {
        StateSession = StateSession.Recovery;
        var tcs = _sessionReadyTcs;
        tcs.TrySetException(ex);
    }

    // 1 if YdbException is an error, we repeat the request
    // 2 OperationCanceledException cancellation
    private async Task<SessionResponse> SendRequest(
        Func<ulong, SessionRequest> requestFactory,
        CancellationToken cancellationToken = default)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var reqId = GetNextReqId();
            var request = requestFactory(reqId);

            var pending = _requestRegistry.Register(reqId, request);

            try
            {
                await SafeWrite(request, cancellationToken);

                await using (cancellationToken.Register(() =>
                                 _requestRegistry.TryCancel(reqId, cancellationToken)))
                {
                    return await pending.Tcs.Task;
                }
            }
            catch (OperationCanceledException)
            {
                _requestRegistry.TryCancel(reqId, cancellationToken);
                throw;
            }
            catch (YdbException ex)
            {
                // retry
                FailSession(ex);
                _requestRegistry.TryCancel(reqId, cancellationToken);
                await _sessionReadyTcs.Task.WaitAsync(cancellationToken);
            }
            catch (Exception)
            {
                _requestRegistry.TryCancel(reqId, cancellationToken);
                throw;
            }
        }
    }

    // 1 if YdbException is an error, we repeat the request
    // 2 OperationCanceledException cancellation
    private async Task<SessionResponse> SendRequestPinned(
        ulong reqId,
        Func<SessionRequest> buildRequest,
        CancellationToken cancellationToken = default)
    {
        var isFirstRequest = true;
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var pending = _requestRegistry.Register(reqId, buildRequest());
            try
            {
                if (!isFirstRequest)
                {
                    isFirstRequest = false;
                }
                else
                {
                    await SafeWrite(buildRequest(), cancellationToken);
                }

                await using (cancellationToken.Register(() =>
                                 _requestRegistry.TryCancel(reqId, cancellationToken)))
                {
                    return await pending.Tcs.Task;
                }
            }
            catch (OperationCanceledException)
            {
                _requestRegistry.TryCancel(reqId, cancellationToken);
                throw;
            }
            catch (YdbException ex)
            {
                // retry
                FailSession(ex);
                _requestRegistry.TryCancel(reqId, cancellationToken);
                await _sessionReadyTcs.Task.WaitAsync(cancellationToken);
            }
            catch (Exception)
            {
                _requestRegistry.TryCancel(reqId, cancellationToken);
                throw;
            }
        }
    }


    /**
     * Gets the next request ID
     */
    private ulong GetNextReqId()
        => _requestRegistry.NextReqId();

    private async Task WaitSessionStopped()
    {
        if (_sessionStopped)
            return;

        try
        {
            await Task.Delay(Timeout.Infinite, _sessionStoppedCts.Token);
        }
        catch (OperationCanceledException)
        {
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;
        try
        {
            if (_stream != null && !_streamClosed)
            {
                try
                {
                    await SendStop();
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to send stop session request");
                }

                await Task.WhenAny(
                    WaitSessionStopped(), Task.Delay(TimeSpan.FromSeconds(60), _cancelTokenSource.Token));
                _disposed = true;
                _streamClosed = true;
            }
        }
        catch (Exception)
        {
            throw new YdbException("Session closing failed");
        }
        finally
        {
            try
            {
                if (_stream != null)
                {
                    await _stream.RequestStreamComplete().ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Error while completing request stream");
            }

            StateSession = StateSession.Closed;
            _initTask.Dispose();
            _requestRegistry.Dispose();
            _watcherRegistry.Dispose();
            _writeLock.Dispose();

            _cancelTokenSource.Dispose();
            _sessionStoppedCts.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
