using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.RequestRegistry;
using Ydb.Sdk.Coordination.Settings;
using Ydb.Sdk.Coordination.Watcher;

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
    private readonly SessionRequestRegistry _requestRegistry = new();
    public StateSession StateSession { get; private set; } = StateSession.Initial;

    private readonly TaskCompletionSource _firstSessionStartedTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly Task _initTask;

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
        _driver = driver;
        _pathNode = pathNode;
        _cancelTokenSource = cancelTokenSource ?? new CancellationTokenSource();
        _initTask = InitializeAsync();
    }


    private async Task InitializeAsync()
    {
        StateSession = StateSession.Connecting;
        _stream = await _driver.BidirectionalStreamCall(CoordinationService.SessionMethod, new GrpcRequestSettings());
        if (_stream == null)
            throw new InvalidOperationException("Stream is null in SendStartSession");
        _ = Task.Run(RunProcessingStreamResponse, _cancelTokenSource.Token);
        await SendStartSession();
        StateSession = StateSession.Connected;
    }


    private async Task EnsureInitialized()
    {
        await _initTask;

        if (_stream == null || _streamClosed)
            throw new InvalidOperationException("Stream is not initialized");
    }

    private async Task RunProcessingStreamResponse()
    {
        try
        {
            while (await _stream!.MoveNextAsync().WaitAsync(_cancelTokenSource.Token))
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
                    case SessionResponse.ResponseOneofCase.AcquireSemaphorePending:
                        HandleAcquireSemaphorePending(response);
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
                    _requestRegistry.TryResolve(reqId.Value, () => ExtractResult(response)!);
                }
            }
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Stream processing failed");
        }
        finally
        {
            await _cancelTokenSource.CancelAsync();
        }
    }


    public CancellationToken Token => _cancelTokenSource.Token;

    public async Task CreateSemaphore(string name, ulong limit, byte[]? data,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating {Name} (limit={Limit})", name, limit);
        await EnsureInitialized();
        var combineToken = LinkToken(cancellationToken);
        var reqId = GetNextReqId();
        var createSemaphore = new SessionRequest
        {
            CreateSemaphore = new SessionRequest.Types.CreateSemaphore
            {
                Name = name,
                Limit = limit,
                Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, createSemaphore, combineToken);
            await task;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            throw new YdbException("Create semaphore failed");
        }
    }


    public async Task UpdateSemaphore(string name, byte[]? data, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Updating data on {Name} ({Bytes} bytes)",
            name,
            data?.Length ?? 0
        );
        await EnsureInitialized();
        var combineToken = LinkToken(cancellationToken);
        var reqId = GetNextReqId();
        var updateSemaphore = new SessionRequest
        {
            UpdateSemaphore = new SessionRequest.Types.UpdateSemaphore
            {
                Name = name,
                Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, updateSemaphore, combineToken);
            await task;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            throw new YdbException("Update semaphore failed");
        }
    }


    public async Task DeleteSemaphore(string name, bool force, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation(
            "Deleting {Name}, force = {Force}",
            name,
            force
        );
        await EnsureInitialized();
        var combineToken = LinkToken(cancellationToken);
        var reqId = GetNextReqId();
        var deleteSemaphore = new SessionRequest
        {
            DeleteSemaphore = new SessionRequest.Types.DeleteSemaphore
            {
                Name = name,
                Force = force,
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, deleteSemaphore, combineToken);
            await task;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            throw new YdbException("Delete semaphore failed");
        }
    }


    public async Task<SemaphoreDescriptionClient> DescribeSemaphore(string name,
        DescribeSemaphoreMode mode, CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var combineToken = LinkToken(cancellationToken);
        var reqId = GetNextReqId();
        var describeSemaphore = new SessionRequest
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
        };

        try
        {
            var response = await SendRequest(reqId, describeSemaphore, combineToken);
            return new SemaphoreDescriptionClient(response.DescribeSemaphoreResult
                .SemaphoreDescription);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            throw new YdbException("Describe semaphore failed");
        }
    }

    public async Task<bool> AcquireSemaphore(string name, ulong count, bool isEphemeral, byte[]? data,
        TimeSpan? timeout, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Waiting to acquire {Name} (count={Count})", name, count);
        await EnsureInitialized();
        var combineToken = LinkToken(cancellationToken);
        var reqId = GetNextReqId();

        var acquireSemaphore = new SessionRequest
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

        try
        {
            var response = await SendRequest(reqId, acquireSemaphore, combineToken);
            return response.AcquireSemaphoreResult.Acquired;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            throw new YdbException("Acquire semaphore failed");
        }
    }

    public async Task ReleaseSemaphore(string name, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Releasing {Name}", name);
        await EnsureInitialized();
        var combineToken = LinkToken(cancellationToken);
        var reqId = GetNextReqId();
        var releaseSemaphore = new SessionRequest
        {
            ReleaseSemaphore = new SessionRequest.Types.ReleaseSemaphore
            {
                Name = name,
                ReqId = reqId
            }
        };

        try
        {
            await SendRequest(reqId, releaseSemaphore, combineToken);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception)
        {
            throw new YdbException("Release semaphore failed");
        }
    }

    public async Task<WatchResult<SemaphoreDescriptionClient>> WatchSemaphore(
        string name,
        DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode,
        CancellationToken cancellationToken = default)
    {
        await EnsureInitialized();
        var combineToken = LinkToken(cancellationToken);
        var subscription = _watcherRegistry.Watch(name);

        var firstResponse = await DescribeSemaphoreInternal(
            name,
            describeMode,
            watchMode,
            subscription,
            combineToken);
        var initial = new SemaphoreDescriptionClient(
            firstResponse.DescribeSemaphoreResult.SemaphoreDescription);
        return new WatchResult<SemaphoreDescriptionClient>(initial, Updates(combineToken));

        async IAsyncEnumerable<SemaphoreDescriptionClient> Updates(
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

                    yield return new SemaphoreDescriptionClient(
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
            var reqId = GetNextReqId();

            var request = new SessionRequest
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

            var response = await SendRequest(reqId, request, token);
            if (response.DescribeSemaphoreResult.WatchAdded)
            {
                _watcherRegistry.RemapWatch(name, subscription, reqId);
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
            SessionResponse.ResponseOneofCase.AcquireSemaphoreResult => response.AcquireSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult => response.ReleaseSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.DescribeSemaphoreResult => response.DescribeSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.CreateSemaphoreResult => response.CreateSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.UpdateSemaphoreResult => response.UpdateSemaphoreResult.ReqId,
            SessionResponse.ResponseOneofCase.DeleteSemaphoreResult => response.DeleteSemaphoreResult.ReqId,
            _ => null
        };


    private static SessionResponse? ExtractResult(SessionResponse response)
    {
        switch (response.ResponseCase)
        {
            case SessionResponse.ResponseOneofCase.AcquireSemaphoreResult:
                Status.FromProto(response.AcquireSemaphoreResult.Status, response.AcquireSemaphoreResult.Issues)
                    .EnsureSuccess();
                return response;

            case SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult:
                Status.FromProto(response.ReleaseSemaphoreResult.Status, response.ReleaseSemaphoreResult.Issues)
                    .EnsureSuccess();
                return response;

            case SessionResponse.ResponseOneofCase.DescribeSemaphoreResult:
                Status.FromProto(response.DescribeSemaphoreResult.Status, response.DescribeSemaphoreResult.Issues)
                    .EnsureSuccess();
                return response;

            case SessionResponse.ResponseOneofCase.CreateSemaphoreResult:
                Status.FromProto(response.CreateSemaphoreResult.Status, response.CreateSemaphoreResult.Issues)
                    .EnsureSuccess();
                return response;

            case SessionResponse.ResponseOneofCase.UpdateSemaphoreResult:
                Status.FromProto(response.UpdateSemaphoreResult.Status, response.UpdateSemaphoreResult.Issues)
                    .EnsureSuccess();
                return response;

            case SessionResponse.ResponseOneofCase.DeleteSemaphoreResult:
                Status.FromProto(response.DeleteSemaphoreResult.Status, response.DeleteSemaphoreResult.Issues)
                    .EnsureSuccess();
                return response;

            default:
                return null;
        }
    }

    private CancellationToken LinkToken(CancellationToken token)
    {
        if (token == CancellationToken.None)
            return _cancelTokenSource.Token;

        return CancellationTokenSource
            .CreateLinkedTokenSource(_cancelTokenSource.Token, token)
            .Token;
    }

    private async Task SendStartSession()
    {
        if (_stream == null)
            throw new InvalidOperationException("Stream not initialized");


        _sessionStartedTcs =
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var key = CreateRandomKey();
        var initRequestStart = new SessionRequest
        {
            SessionStart = new SessionRequest.Types.SessionStart
            {
                SessionId = SessionId,
                Path = _pathNode,
                Description = _sessionOptions.Description,
                TimeoutMillis = (ulong)_sessionOptions.StartTimeout.TotalMilliseconds,
                ProtectionKey = key,
                SeqNo = _seqNo++
            }
        };

        await SafeWrite(initRequestStart, _cancelTokenSource.Token);

        await _sessionStartedTcs.Task;
    }

    private async Task SendStop()
    {
        await EnsureInitialized();
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

    private void HandleAcquireSemaphorePending(SessionResponse response)
        => _logger.LogTrace("Session got acquire semaphore pending msg {ReqId}",
            response.AcquireSemaphorePending.ReqId);


    private void HandleSemaphoreChanged(SessionResponse.Types.DescribeSemaphoreChanged change)
        => _watcherRegistry.Notify(change);


    private static ByteString CreateRandomKey()
    {
        var protectionKey = new byte[16];
        RandomNumberGenerator.Fill(protectionKey);
        return ByteString.CopyFrom(protectionKey);
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

    private async Task<SessionResponse> SendRequest(ulong reqId, SessionRequest request,
        CancellationToken cancellationToken = default)
    {
        var pending = _requestRegistry.Register(reqId, request);

        try
        {
            await SafeWrite(request, cancellationToken);
        }
        catch (Exception e)
        {
            _requestRegistry.TryCancel(reqId, cancellationToken);
            pending.Tcs.TrySetException(e);
            throw;
        }

        await using (cancellationToken.Register(() =>
                         _requestRegistry.TryCancel(reqId, cancellationToken)))
        {
            return await pending.Tcs.Task;
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

        _disposed = true;
        _streamClosed = true;

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
            _requestRegistry.Dispose();
            _watcherRegistry.Dispose();
            _writeLock.Dispose();

            _cancelTokenSource.Dispose();
            _sessionStoppedCts.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}
