// using System.Collections.Concurrent;
// using Xunit;
// using Ydb.Query;
// using Ydb.Sdk.Ado.Session;
//
// namespace Ydb.Sdk.Ado.Tests.Session;
//
// public class PoolingSessionSourceMockTests
// {
//     [Fact]
//     public void MinSessionPool_bigger_than_MaxSessionPool_throws() => Assert.Throws<ArgumentException>(() =>
//         new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(1),
//             new YdbConnectionStringBuilder { MaxSessionPool = 1, MinSessionPool = 2 })
//     );
//
//     [Fact]
//     public async Task Reuse_Session_Before_Creating_new()
//     {
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(1),
//             new YdbConnectionStringBuilder());
//         var session = await sessionSource.OpenSession();
//         var sessionId = session.SessionId();
//         session.Close();
//         session = await sessionSource.OpenSession();
//         Assert.Equal(sessionId, session.SessionId());
//     }
//
//     [Fact]
//     public async Task Creating_Session_Throw_Exception()
//     {
//         for (var it = 0; it < 10_000; it++)
//         {
//             const string errorMessage = "Error on open session";
//             const int maxSessionSize = 200;
//
//             var mockPoolingSessionFactory = new MockPoolingSessionFactory(maxSessionSize)
//             {
//                 Open = sessionNum =>
//                     sessionNum <= maxSessionSize * 2
//                         ? Task.FromException(new YdbException(errorMessage))
//                         : Task.CompletedTask
//             };
//
//             var sessionSource = new PoolingSessionSource<MockPoolingSession>(
//                 mockPoolingSessionFactory, new YdbConnectionStringBuilder { MaxSessionPool = maxSessionSize }
//             );
//
//             var tasks = new List<Task>();
//             var countSuccess = 0;
//
//             for (var i = 0; i < maxSessionSize * 4; i++)
//             {
//                 tasks.Add(Task.Run(async () =>
//                 {
//                     try
//                     {
//                         var session = await sessionSource.OpenSession();
//                         // ReSharper disable once AccessToModifiedClosure
//                         Interlocked.Increment(ref countSuccess);
//                         Assert.True(session.SessionId() > maxSessionSize * 2);
//                         session.Close();
//                     }
//                     catch (YdbException e)
//                     {
//                         Assert.Equal(errorMessage, e.Message);
//                     }
//                 }));
//             }
//
//             await Task.WhenAll(tasks);
//             Assert.Equal(maxSessionSize * 2, Volatile.Read(ref countSuccess));
//             Assert.True(maxSessionSize * 3 >= mockPoolingSessionFactory.SessionOpenedCount);
//             Assert.True(maxSessionSize * 2 < mockPoolingSessionFactory.SessionOpenedCount);
//         }
//     }
//
//     [Fact]
//     public async Task HighContention_OpenClose_NotCanceledException()
//     {
//         const int highContentionTasks = 100;
//         const int maxSessionSize = highContentionTasks / 2;
//         var mockPoolingSessionFactory = new MockPoolingSessionFactory(maxSessionSize);
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(
//             mockPoolingSessionFactory, new YdbConnectionStringBuilder { MaxSessionPool = maxSessionSize }
//         );
//
//         for (var it = 0; it < 100_000; it++)
//         {
//             var tasks = new Task[highContentionTasks];
//
//             for (var i = 0; i < highContentionTasks; i++)
//             {
//                 tasks[i] = Task.Run(async () =>
//                 {
//                     var session = await sessionSource.OpenSession();
//                     Assert.True(session.SessionId() <= maxSessionSize);
//                     await Task.Yield();
//                     session.Close();
//                 });
//             }
//
//             await Task.WhenAll(tasks);
//         }
//     }
//
//     [Fact]
//     public async Task DisposeAsync_Cancel_WaitersSession()
//     {
//         const int maxSessionSize = 10;
//         var mockFactory = new MockPoolingSessionFactory(maxSessionSize);
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(
//             mockFactory, new YdbConnectionStringBuilder { MaxSessionPool = maxSessionSize }
//         );
//
//         var openSessions = new List<ISession>();
//         var waitingSessionTasks = new List<Task>();
//         for (var i = 0; i < maxSessionSize; i++)
//         {
//             openSessions.Add(await sessionSource.OpenSession());
//         }
//
//         for (var i = 0; i < maxSessionSize; i++)
//         {
//             waitingSessionTasks.Add(Task.Run(async () =>
//             {
//                 var session = await sessionSource.OpenSession();
//                 session.Close();
//             }));
//         }
//
//         var disposeTask = Task.Run(async () => await sessionSource.DisposeAsync());
//         Assert.Equal(maxSessionSize, mockFactory.NumSession);
//         await Task.Delay(5_000);
//         for (var i = 0; i < maxSessionSize; i++)
//         {
//             openSessions[i].Close();
//         }
//
//         await disposeTask;
//         Assert.Equal(0, mockFactory.NumSession);
//         for (var i = 0; i < maxSessionSize; i++)
//         {
//             Assert.Equal("The session source has been shut down.",
//                 (await Assert.ThrowsAsync<YdbException>(() => waitingSessionTasks[i])).Message);
//         }
//
//         Assert.Equal("The session source has been shut down.",
//             (await Assert.ThrowsAsync<YdbException>(async () => await sessionSource.OpenSession())).Message);
//     }
//
//     [Fact]
//     public async Task StressTest_DisposeAsync_Close_Driver()
//     {
//         const int contentionTasks = 200;
//         const int maxSessionSize = 100;
//         for (var it = 0; it < 100_000; it++)
//         {
//             var disposeCalled = false;
//             var mockFactory = new MockPoolingSessionFactory(maxSessionSize)
//             {
//                 Dispose = () =>
//                 {
//                     Volatile.Write(ref disposeCalled, true);
//                     return ValueTask.CompletedTask;
//                 }
//             };
//             var settings = new YdbConnectionStringBuilder { MaxSessionPool = maxSessionSize };
//             var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
//             var openSessionTasks = new List<Task>();
//             for (var i = 0; i < contentionTasks; i++)
//             {
//                 var itCopy = it;
//                 var iCopy = i;
//
//                 openSessionTasks.Add(Task.Run(async () =>
//                 {
//                     if (itCopy % contentionTasks == iCopy)
//                     {
//                         await sessionSource.DisposeAsync();
//                         return;
//                     }
//
//                     try
//                     {
//                         var session = await sessionSource.OpenSession();
//                         await Task.Yield();
//                         session.Close();
//                     }
//                     catch (YdbException e)
//                     {
//                         Assert.Equal("The session source has been shut down.", e.Message);
//                     }
//                     catch (OperationCanceledException)
//                     {
//                     }
//                 }));
//             }
//
//             await Task.WhenAll(openSessionTasks);
//             Assert.Equal(0, mockFactory.NumSession);
//             Assert.True(disposeCalled);
//         }
//     }
//
//     [Fact]
//     public async Task IdleTimeout_MinSessionSize_CloseNumSessionsMinusMinSessionCount()
//     {
//         const int maxSessionSize = 50;
//         const int minSessionSize = 10;
//         const int idleTimeoutSeconds = 1;
//
//         var mockFactory = new MockPoolingSessionFactory(maxSessionSize);
//         var settings = new YdbConnectionStringBuilder
//         {
//             SessionIdleTimeout = idleTimeoutSeconds,
//             MaxSessionPool = maxSessionSize,
//             MinSessionPool = minSessionSize
//         };
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
//
//         var openSessions = new List<ISession>();
//         for (var it = 0; it < maxSessionSize; it++)
//         {
//             openSessions.Add(await sessionSource.OpenSession());
//         }
//
//         foreach (var it in openSessions)
//         {
//             it.Close();
//         }
//
//         await Task.Delay(TimeSpan.FromSeconds(idleTimeoutSeconds * 5)); // cleaning idle sessions
//         Assert.Equal(minSessionSize, mockFactory.NumSession);
//
//         var openSessionTasks = new List<Task<ISession>>();
//         for (var it = 0; it < minSessionSize; it++)
//         {
//             openSessionTasks.Add(Task.Run(async () => await sessionSource.OpenSession()));
//         }
//
//         foreach (var it in openSessionTasks)
//         {
//             (await it).Close();
//         }
//
//         Assert.Equal(minSessionSize, mockFactory.NumSession);
//         Assert.Equal(maxSessionSize, mockFactory.SessionOpenedCount);
//     }
//
//     [Fact]
//     public async Task StressTest_HighContention_OpenClose()
//     {
//         var cts = new CancellationTokenSource();
//         cts.CancelAfter(TimeSpan.FromMinutes(1));
//
//         const int maxSessionSize = 50;
//         const int minSessionSize = 10;
//         const int highContentionTasks = maxSessionSize * 5;
//         var sessionIdIsBroken = new ConcurrentDictionary<int, bool>();
//
//         var mockFactory = new MockPoolingSessionFactory(maxSessionSize)
//         {
//             IsBroken = sessionNum =>
//             {
//                 var isBroken = Random.Shared.NextDouble() < 0.2;
//                 sessionIdIsBroken[sessionNum] = isBroken;
//                 return isBroken;
//             },
//             Open = sessionNum =>
//             {
//                 sessionIdIsBroken[sessionNum] = false;
//                 return Task.CompletedTask;
//             }
//         };
//         var settings = new YdbConnectionStringBuilder
//             { MaxSessionPool = maxSessionSize, MinSessionPool = minSessionSize };
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
//
//         var workers = new List<Task>();
//         for (var it = 0; it < highContentionTasks; it++)
//         {
//             workers.Add(Task.Run(async () =>
//             {
//                 try
//                 {
//                     while (!cts.IsCancellationRequested)
//                     {
//                         var session = await sessionSource.OpenSession(cts.Token);
//                         Assert.False(sessionIdIsBroken[session.SessionId()]);
//                         session.Close();
//                         await Task.Delay(Random.Shared.Next(maxSessionSize), cts.Token);
//                     }
//                 }
//                 catch (OperationCanceledException)
//                 {
//                 }
//                 catch (YdbException)
//                 {
//                 }
//             }, cts.Token));
//         }
//
//         await Task.WhenAll(workers);
//     }
//
//     [Fact]
//     public async Task Get_Session_From_Exhausted_Pool()
//     {
//         var mockFactory = new MockPoolingSessionFactory(1);
//         var settings = new YdbConnectionStringBuilder
//         {
//             MaxSessionPool = 1,
//             MinSessionPool = 0
//         };
//
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
//         var session = await sessionSource.OpenSession();
//         var cts = new CancellationTokenSource();
//         cts.CancelAfter(500);
//
//         Assert.Equal("The connection pool has been exhausted, either raise 'MaxSessionPool' (currently 1) " +
//                      "or 'CreateSessionTimeout' (currently 5 seconds) in your connection string.",
//             (await Assert.ThrowsAsync<YdbException>(async () => await sessionSource.OpenSession(cts.Token))).Message);
//         session.Close();
//
//         Assert.Equal(1, mockFactory.NumSession);
//         Assert.Equal(1, mockFactory.SessionOpenedCount);
//     }
//
//     [Fact]
//     public async Task Return_IsBroken_Session()
//     {
//         const int maxSessionSize = 10;
//         var mockFactory = new MockPoolingSessionFactory(maxSessionSize) { IsBroken = _ => true };
//         var settings = new YdbConnectionStringBuilder
//         {
//             MaxSessionPool = maxSessionSize,
//             MinSessionPool = 0
//         };
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
//
//         for (var it = 0; it < maxSessionSize * 2; it++)
//         {
//             var session = await sessionSource.OpenSession();
//             session.Close();
//         }
//
//         Assert.Equal(0, mockFactory.NumSession);
//         Assert.Equal(maxSessionSize * 2, mockFactory.SessionOpenedCount);
//     }
//
//     [Fact]
//     public async Task CheckIdleSession_WhenIsBrokenInStack_CreateNewSession()
//     {
//         var isBroken = false;
//         const int maxSessionSize = 10;
//         // ReSharper disable once AccessToModifiedClosure
//         var mockFactory = new MockPoolingSessionFactory(maxSessionSize) { IsBroken = _ => isBroken };
//         var settings = new YdbConnectionStringBuilder
//         {
//             MaxSessionPool = maxSessionSize,
//             MinSessionPool = 0
//         };
//         var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
//
//         var openSessions = new List<ISession>();
//         for (var it = 0; it < maxSessionSize; it++)
//         {
//             openSessions.Add(await sessionSource.OpenSession());
//         }
//
//         foreach (var session in openSessions)
//         {
//             session.Close();
//         }
//
//         Assert.Equal(maxSessionSize, mockFactory.NumSession);
//
//         isBroken = true;
//         for (var it = 0; it < maxSessionSize; it++)
//         {
//             var session = await sessionSource.OpenSession();
//             isBroken = false;
//             session.Close();
//         }
//
//         Assert.Equal(1, mockFactory.NumSession);
//         Assert.Equal(maxSessionSize + 1, mockFactory.SessionOpenedCount);
//     }
// }
//
// internal static class ISessionExtension
// {
//     internal static int SessionId(this ISession session) => ((MockPoolingSession)session).SessionId;
// }
//
// internal class MockPoolingSessionFactory(int maxSessionSize) : IPoolingSessionFactory<MockPoolingSession>
// {
//     private int _sessionOpened;
//     private int _numSession;
//
//     internal int SessionOpenedCount => Volatile.Read(ref _sessionOpened);
//     internal int NumSession => Volatile.Read(ref _numSession);
//
//     internal Func<int, Task> Open { private get; init; } = _ => Task.CompletedTask;
//     internal Func<int, bool> IsBroken { private get; init; } = _ => false;
//     internal Func<ValueTask> Dispose { private get; init; } = () => ValueTask.CompletedTask;
//
//     public MockPoolingSession NewSession(PoolingSessionSource<MockPoolingSession> source) =>
//         new(source,
//             async sessionCountOpened =>
//             {
//                 await Open(sessionCountOpened);
//
//                 Assert.True(Interlocked.Increment(ref _numSession) <= maxSessionSize);
//
//                 await Task.Yield();
//             },
//             () =>
//             {
//                 Assert.True(Interlocked.Decrement(ref _numSession) >= 0);
//
//                 return Task.CompletedTask;
//             },
//             sessionNum => IsBroken(sessionNum),
//             Interlocked.Increment(ref _sessionOpened)
//         );
//
//     public ValueTask DisposeAsync() => Dispose();
// }
//
// internal class MockPoolingSession(
//     PoolingSessionSource<MockPoolingSession> source,
//     Func<int, Task> mockOpen,
//     Func<Task> mockDeleteSession,
//     Func<int, bool> mockIsBroken,
//     int sessionNum
// ) : PoolingSessionBase<MockPoolingSession>(source)
// {
//     public int SessionId => sessionNum;
//     public override IDriver Driver => null!;
//     public override bool IsBroken => mockIsBroken(sessionNum);
//
//     internal override Task Open(CancellationToken cancellationToken) => mockOpen(sessionNum);
//     internal override Task DeleteSession() => mockDeleteSession();
//
//     public override ValueTask<IServerStream<ExecuteQueryResponsePart>> ExecuteQuery(
//         string query,
//         Dictionary<string, TypedValue> parameters,
//         GrpcRequestSettings settings,
//         TransactionControl? txControl
//     ) => throw new NotImplementedException();
//
//     public override Task CommitTransaction(string txId, CancellationToken cancellationToken = default) =>
//         throw new NotImplementedException();
//
//     public override Task RollbackTransaction(string txId, CancellationToken cancellationToken = default) =>
//         throw new NotImplementedException();
//
//     public override void OnNotSuccessStatusCode(StatusCode code)
//     {
//     }
// }
