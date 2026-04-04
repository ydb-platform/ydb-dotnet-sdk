using System.Collections.Concurrent;
using System.Security.Cryptography;
using Google.Protobuf;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Dto;
using Ydb.Sdk.Coordination.Settings;
using Ydb.Sdk.Coordination.Watcher;
using SemaphoreDescription = Ydb.Coordination.SemaphoreDescription;

namespace Ydb.Sdk.Coordination;

public class SessionRuntime
{
    private readonly IDriver _driver;
    //private readonly YdbRetryPolicyExecutor _ydbRetryPolicyExecutor;


    private ulong _sessionId;
    private ulong _reqIdCounter;
    private ulong _seqNo;
    private readonly string _pathNode;


    // Tasks for waiting SessionStarted and SessionStopped
    private TaskCompletionSource? _sessionStartedTcs;
    private TaskCompletionSource? _sessionStoppedTcs;

    // Bidirectional stream handler
    private IBidirectionalStream<SessionRequest, SessionResponse>? _stream;
    private readonly ConcurrentDictionary<ulong, PendingRequest<PendingResult>> _pendingRequests = new();
    //private readonly List<SessionRequest> _fireAndForgetRequests = new();

    // Reconnection state
    //private bool _closed = false;

    // Map of _reqIdCounter to semaphore name for watch tracking
    //private readonly ConcurrentDictionary<ulong, string> _watchedSemaphores = new();


    private readonly TaskCompletionSource _firstSessionStartedTcs =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    // EVENTS
    //public event Action<SessionExpiredEvent>? SessionExpired;
    //public event Action<SemaphoreChangedEvent>? SemaphoreChanged;    

    // private readonly CancellationTokenSource _disposeCts = new();
    private readonly SemaphoreSlim _writeLock = new(1, 1);
    private readonly Task _initTask;

    private volatile bool _disposed;
    private volatile bool _streamClosed;

    private readonly WatcherRegistry _watcherRegistry = new WatcherRegistry();

    public SessionRuntime(IDriver driver, string pathNode) //IRetryPolicy retryPolicy
    {
        //_ydbRetryPolicyExecutor = new YdbRetryPolicyExecutor(retryPolicy);
        _driver = driver;
        _pathNode = pathNode;

        _initTask = InitializeAsync();
    }

    public ulong SessionId => _sessionId;

    private async Task InitializeAsync()
        => await StreamCall();


    private async Task StreamCall()
    {
        _stream = await _driver.BidirectionalStreamCall(CoordinationService.SessionMethod, new GrpcRequestSettings());
        if (_stream == null)
            throw new InvalidOperationException("Stream is null in SendStartSession");
        _ = Task.Run(RunProcessingStreamResponse);
        await SendStartSession();
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
            while (await _stream!.MoveNextAsync())
            {
                var response = _stream.Current;

                switch (response.ResponseCase)
                {
                    case SessionResponse.ResponseOneofCase.Ping:
                        // Server sent ping, respond with pong
                        await HandlePing(response);
                        break;
                    case SessionResponse.ResponseOneofCase.Failure:
                        await HandleFailure(response);
                        break;
                    case SessionResponse.ResponseOneofCase.SessionStarted:
                        HandleSessionStarted(); //HandleSessionStarted(response);
                        break;
                    case SessionResponse.ResponseOneofCase.SessionStopped:
                        HandleSessionStopped(); //HandleSessionStopped(response)
                        break;
                    case SessionResponse.ResponseOneofCase.AcquireSemaphorePending:
                        // note
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
                if (reqId.HasValue &&
                    _pendingRequests.TryRemove(reqId.Value, out var pending))
                {
                    try
                    {
                        var result = ExtractResult(response);
                        pending.Tcs.TrySetResult(result!);
                    }
                    catch (Exception ex)
                    {
                        pending.Tcs.TrySetException(ex);
                    }
                }
            }
        }
        catch (Exception ex)
        {
            FailAllPending(ex);
        }
        finally
        {
            _streamClosed = true;
            FailAllPending(new Exception("Session stream closed"));
        }
    }

    private void FailAllPending(Exception ex)
    {
        foreach (var (_, pending) in _pendingRequests)
        {
            pending.Tcs.TrySetException(ex);
        }

        _pendingRequests.Clear();
    }

    /*
     * private void TryCompletePending(SessionResponse response)
{
    var reqId = ExtractReqId(response);
    if (!reqId.HasValue)
        return;

    if (_pendingRequests.TryRemove(reqId.Value, out var pending))
    {
        try
        {
            var result = ExtractResult(response);

            if (result == null)
            {
                pending.Tcs.TrySetException(
                    new InvalidOperationException($"Unexpected response type: {response.ResponseCase}"));
                return;
            }

            pending.Tcs.TrySetResult(result);
        }
        catch (Exception ex)
        {
            pending.Tcs.TrySetException(ex);
        }
    }
}
     */

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

    // возвращать пару response и код и снаружи 


    private static PendingResult? ExtractResult(SessionResponse response)
    {
        void EnsureSuccess(StatusIds.Types.StatusCode status, object issues)
        {
            if (status != StatusIds.Types.StatusCode.Success)
                throw new Exception($"{status} {issues}");
        }

        switch (response.ResponseCase)
        {
            case SessionResponse.ResponseOneofCase.AcquireSemaphoreResult:
                EnsureSuccess(response.AcquireSemaphoreResult.Status, response.AcquireSemaphoreResult.Issues);
                return new PendingResult(response, SessionResponse.ResponseOneofCase.AcquireSemaphoreResult);

            case SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult:
                EnsureSuccess(response.ReleaseSemaphoreResult.Status, response.ReleaseSemaphoreResult.Issues);
                return new PendingResult(response, SessionResponse.ResponseOneofCase.ReleaseSemaphoreResult);

            case SessionResponse.ResponseOneofCase.DescribeSemaphoreResult:
                EnsureSuccess(response.DescribeSemaphoreResult.Status, response.DescribeSemaphoreResult.Issues);
                return new PendingResult(response, SessionResponse.ResponseOneofCase.DescribeSemaphoreResult);

            case SessionResponse.ResponseOneofCase.CreateSemaphoreResult:
                EnsureSuccess(response.CreateSemaphoreResult.Status, response.CreateSemaphoreResult.Issues);
                return new PendingResult(response, SessionResponse.ResponseOneofCase.CreateSemaphoreResult);

            case SessionResponse.ResponseOneofCase.UpdateSemaphoreResult:
                EnsureSuccess(response.UpdateSemaphoreResult.Status, response.UpdateSemaphoreResult.Issues);
                return new PendingResult(response, SessionResponse.ResponseOneofCase.UpdateSemaphoreResult);

            case SessionResponse.ResponseOneofCase.DeleteSemaphoreResult:
                EnsureSuccess(response.DeleteSemaphoreResult.Status, response.DeleteSemaphoreResult.Issues);
                return new PendingResult(response, SessionResponse.ResponseOneofCase.DeleteSemaphoreResult);

            default:
                return null;
        }
    }

    private async Task SafeWrite(SessionRequest request)
    {
        await _writeLock.WaitAsync();
        try
        {
            await _stream!.Write(request);
        }
        finally
        {
            _writeLock.Release();
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
        await SafeWrite(pongRequest);
    }

    private async Task HandleFailure(SessionResponse response)
    {
        var failure = response.Failure;
        if ((failure.Status == StatusIds.Types.StatusCode.BadSession) |
            (failure.Status == StatusIds.Types.StatusCode.SessionExpired))
        {
            // If session is expired or not accessible, reset session ID to create a new session on reconnect
            //var expiredId = _sessionId;
            // _sessionId = 0;
            //_watchedSemaphores.Clear();

            // // Emit sessionExpired event to notify user
        }

        await _stream!.RequestStreamComplete();
    }

    private void HandleSessionStarted() //SessionResponse response
    {
        //_sessionId = response.SessionStarted.SessionId;
        // Resolve the sessionStarted promise
        _sessionStartedTcs?.TrySetResult();
        _sessionStartedTcs = null;

        // Resolve the first session started promise (only once)
        _firstSessionStartedTcs.TrySetResult();
    }

    private void HandleSessionStopped() //SessionResponse response
    {
        //_sessionId = response.SessionStopped.SessionId; // в логи записываем
        _sessionStoppedTcs?.TrySetResult();
        _sessionStoppedTcs = null;
    }


    private void HandleSemaphoreChanged(SessionResponse.Types.DescribeSemaphoreChanged change)
        => _watcherRegistry.Notify(change);


    private async Task SendStartSession()
    {
        if (_stream == null)
            throw new InvalidOperationException("Stream not initialized");


        _sessionStartedTcs =
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        _sessionId = 0;
        var reqId = GetNextReqId();
        var node = _pathNode;
        // var timeout = new TimeSpan(5);
        var key = CreateRandomKey();
        var initRequestStart = new SessionRequest
        {
            SessionStart = new SessionRequest.Types.SessionStart
            {
                SessionId = 0,
                Path = node,
                TimeoutMillis = 5000,
                ProtectionKey = key,
                SeqNo = _seqNo++
            }
        };

        await SafeWrite(initRequestStart);

        await _sessionStartedTcs.Task;
    }

    private static ByteString CreateRandomKey()
    {
        var protectionKey = new byte[16];
        RandomNumberGenerator.Fill(protectionKey);
        return ByteString.CopyFrom(protectionKey);
    }

    private async Task<PendingResult> SendRequest(ulong reqId, SessionRequest request,
        CancellationToken cancellationToken = default)
    {
        var tcs = new TaskCompletionSource<PendingResult>(
            TaskCreationOptions.RunContinuationsAsynchronously);

        _pendingRequests[reqId] = new PendingRequest<PendingResult>(tcs);

        try
        {
            await SafeWrite(request);
        }
        catch (Exception e)
        {
            _pendingRequests.TryRemove(reqId, out _);
            tcs.TrySetException(e);
            throw;
        }

        await using (cancellationToken.Register(() =>
                     {
                         if (_pendingRequests.TryRemove(reqId, out var pending))
                         {
                             pending.Tcs.TrySetCanceled(cancellationToken);
                         }
                     }))
        {
            // timeout + cancellation?
            return await tcs.Task;
        }
    }

    /**
     * Gets the next request ID
     */
    private ulong GetNextReqId()
        => Interlocked.Increment(ref _reqIdCounter);


    public async Task CreateSemaphore(string name, ulong limit, byte[]? data)
    {
        await EnsureInitialized();
        var reqId = GetNextReqId();
        var createSemaphore = new SessionRequest
        {
            CreateSemaphore = new SessionRequest.Types.CreateSemaphore()
            {
                Name = name,
                Limit = limit,
                Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, createSemaphore);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Create semaphore failed");
        }
    }


    public async Task UpdateSemaphore(string name, byte[]? data)
    {
        await EnsureInitialized();
        var reqId = GetNextReqId();
        var updateSemaphore = new SessionRequest
        {
            UpdateSemaphore = new SessionRequest.Types.UpdateSemaphore()
            {
                Name = name,
                Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, updateSemaphore);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Update semaphore failed");
        }
    }


    public async Task DeleteSemaphore(string name, bool force)
    {
        await EnsureInitialized();
        var reqId = GetNextReqId();
        var deleteSemaphore = new SessionRequest
        {
            DeleteSemaphore = new SessionRequest.Types.DeleteSemaphore()
            {
                Name = name,
                Force = force,
                ReqId = reqId
            }
        };
        try
        {
            var task = SendRequest(reqId, deleteSemaphore);
            await task;
        }
        catch (Exception)
        {
            throw new YdbException("Delete semaphore failed");
        }
    }


    public async Task<SemaphoreDescription> DescribeSemaphore(string name,
        DescribeSemaphoreMode mode)
    {
        await EnsureInitialized();
        var reqId = GetNextReqId();
        var describeSemaphore = new SessionRequest
        {
            DescribeSemaphore = new SessionRequest.Types.DescribeSemaphore()
            {
                Name = name,
                IncludeOwners = DescribeSemaphoreModeUtils.IncludeOwners(mode),
                IncludeWaiters = DescribeSemaphoreModeUtils.IncludeWaiters(mode),
                WatchData = false,
                WatchOwners = false,
                ReqId = reqId
            }
        };

        try
        {
            var task = await SendRequest(reqId, describeSemaphore);
            return new SemaphoreDescription(task.Request.DescribeSemaphoreResult.SemaphoreDescription);
        }
        catch (Exception)
        {
            throw new YdbException("Describe semaphore failed");
        }
    }

    public async Task AcquireSemaphore(string name, ulong count, bool isEphemeral, byte[]? data,
        TimeSpan timeout)
    {
        await EnsureInitialized();
        var reqId = GetNextReqId();
        var acquireSemaphore = new SessionRequest
        {
            AcquireSemaphore = new SessionRequest.Types.AcquireSemaphore()
            {
                Name = name,
                Count = count, // обратить на число
                Data = data == null ? ByteString.Empty : ByteString.CopyFrom(data),
                Ephemeral = isEphemeral,
                TimeoutMillis = (ulong)timeout.TotalMilliseconds,
                ReqId = reqId
            }
        };

        try
        {
            var task = await SendRequest(reqId, acquireSemaphore);

            // доделать нюансы со взятием семафора
        }
        catch (Exception)
        {
            throw new YdbException("Acquire semaphore failed");
        }
    }

    public async Task ReleaseSemaphore(string name)
    {
        await EnsureInitialized();
        var reqId = GetNextReqId();
        var releaseSemaphore = new SessionRequest
        {
            ReleaseSemaphore = new SessionRequest.Types.ReleaseSemaphore()
            {
                Name = name,
                ReqId = reqId
            }
        };

        try
        {
            await SendRequest(reqId, releaseSemaphore);
        }
        catch (Exception)
        {
            throw new YdbException("Release semaphore failed");
        }
    }

    private async Task SendStop()
    {
        await EnsureInitialized();
        var stopSession = new SessionRequest()
        {
            SessionStop = new SessionRequest.Types.SessionStop()
        };
        try
        {
            await SafeWrite(stopSession);
        }
        catch (Exception)
        {
            throw new YdbException("Stop session failed");
        }
    }

    public async ValueTask Dispose()
    {
        if (_disposed)
            return;

        _disposed = true;

        try
        {
            if (_stream != null)
            {
                await SendStop();
                await _stream.RequestStreamComplete().ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
        }

        // добавить отключение подписок
        _writeLock.Dispose();
    }


    public async Task<SemaphoreWatcher> WatchSemaphore(string name, DescribeSemaphoreMode describeMode,
        WatchSemaphoreMode watchMode)
    {
        await EnsureInitialized();
        var subscription = _watcherRegistry.Watch(name);
        var reqId = GetNextReqId();
        var watchSemaphore = new SessionRequest()
        {
            DescribeSemaphore = new SessionRequest.Types.DescribeSemaphore()
            {
                Name = name,
                IncludeOwners = DescribeSemaphoreModeUtils.IncludeOwners(describeMode),
                IncludeWaiters = DescribeSemaphoreModeUtils.IncludeWaiters(describeMode),
                WatchData = WatchSemaphoreModeUtils.WatchData(watchMode),
                WatchOwners = WatchSemaphoreModeUtils.WatchOwners(watchMode),
                ReqId = reqId
            }
        };

        try
        {
            var task = await SendRequest(reqId, watchSemaphore);
            var sessionResponse = task.Request;
            if (sessionResponse.DescribeSemaphoreResult.WatchAdded)
            {
                _watcherRegistry.RemapWatch(name, subscription, reqId);
            }

            return new SemaphoreWatcher(
                new Description.SemaphoreDescription(sessionResponse.DescribeSemaphoreResult.SemaphoreDescription),
                subscription);
        }
        catch (Exception)
        {
            _watcherRegistry.RemoveWatch(name, subscription);
            throw new YdbException("Watch semaphore failed");
        }
    }

/*

    public async void AcquireSemaphore(string name, long count, byte[] data,
        TimeSpan timeout)
    {
    }


    public async void AcquireEphemeralSemaphore(String name, bool exclusive,
        byte[] data, TimeSpan timeout)
    {
    }


    */
}
