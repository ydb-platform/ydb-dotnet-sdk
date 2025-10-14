using System.Collections.Concurrent;
using Xunit;
using Ydb.Sdk.Ado.Session;

namespace Ydb.Sdk.Ado.Tests.Session;

public class PoolingSessionSourceMockTests
{
    [Fact]
    public void MinSessionPool_bigger_than_MaxPoolSize_throws() => Assert.Throws<ArgumentException>(() =>
        new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(1),
            new YdbConnectionStringBuilder { MaxPoolSize = 1, MinPoolSize = 2 })
    );

    [Fact]
    public async Task Reuse_Session_Before_Creating_new()
    {
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(new MockPoolingSessionFactory(1),
            new YdbConnectionStringBuilder());
        int sessionId;
        using (var session = await sessionSource.OpenSession())
        {
            sessionId = session.SessionId();
        }

        using (var session = await sessionSource.OpenSession())
        {
            Assert.Equal(sessionId, session.SessionId());
        }
    }

    [Fact]
    public async Task Creating_Session_Throw_Exception()
    {
        for (var it = 0; it < 10_000; it++)
        {
            const string errorMessage = "Error on open session";
            const int maxPoolSize = 200;

            var mockPoolingSessionFactory = new MockPoolingSessionFactory(maxPoolSize)
            {
                Open = sessionNum =>
                    sessionNum <= maxPoolSize * 2
                        ? Task.FromException(new YdbException(errorMessage))
                        : Task.CompletedTask
            };

            var sessionSource = new PoolingSessionSource<MockPoolingSession>(
                mockPoolingSessionFactory, new YdbConnectionStringBuilder { MaxPoolSize = maxPoolSize }
            );

            var tasks = new List<Task>();
            var countSuccess = 0;

            for (var i = 0; i < maxPoolSize * 4; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    try
                    {
                        using var session = await sessionSource.OpenSession();
                        // ReSharper disable once AccessToModifiedClosure
                        Interlocked.Increment(ref countSuccess);
                        Assert.True(session.SessionId() > maxPoolSize * 2);
                    }
                    catch (YdbException e)
                    {
                        Assert.Equal(errorMessage, e.Message);
                    }
                }));
            }

            await Task.WhenAll(tasks);
            Assert.Equal(maxPoolSize * 2, Volatile.Read(ref countSuccess));
            Assert.True(maxPoolSize * 3 >= mockPoolingSessionFactory.SessionOpenedCount);
            Assert.True(maxPoolSize * 2 < mockPoolingSessionFactory.SessionOpenedCount);
        }
    }

    [Fact]
    public async Task HighContention_OpenClose_NotCanceledException()
    {
        const int highContentionTasks = 100;
        const int maxPoolSize = highContentionTasks / 2;
        var mockPoolingSessionFactory = new MockPoolingSessionFactory(maxPoolSize);
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(
            mockPoolingSessionFactory, new YdbConnectionStringBuilder { MaxPoolSize = maxPoolSize }
        );

        for (var it = 0; it < 100_000; it++)
        {
            var tasks = new Task[highContentionTasks];

            for (var i = 0; i < highContentionTasks; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    using var session = await sessionSource.OpenSession();
                    Assert.True(session.SessionId() <= maxPoolSize);
                    await Task.Yield();
                });
            }

            await Task.WhenAll(tasks);
        }
    }

    [Fact]
    public async Task DisposeAsync_Cancel_WaitersSession()
    {
        const int maxPoolSize = 10;
        var mockFactory = new MockPoolingSessionFactory(maxPoolSize);
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(
            mockFactory, new YdbConnectionStringBuilder { MaxPoolSize = maxPoolSize }
        );

        var openSessions = new List<ISession>();
        var waitingSessionTasks = new List<Task>();
        for (var i = 0; i < maxPoolSize; i++)
        {
            openSessions.Add(await sessionSource.OpenSession());
        }

        for (var i = 0; i < maxPoolSize; i++)
        {
            waitingSessionTasks.Add(Task.Run(async () =>
            {
                using var session = await sessionSource.OpenSession();
            }));
        }

        var disposeTask = Task.Run(async () => await sessionSource.DisposeAsync());
        Assert.Equal(maxPoolSize, mockFactory.NumSession);
        await Task.Delay(5_000);
        for (var i = 0; i < maxPoolSize; i++)
        {
            openSessions[i].Dispose();
        }

        await disposeTask;
        Assert.Equal(0, mockFactory.NumSession);
        for (var i = 0; i < maxPoolSize; i++)
        {
            Assert.StartsWith("The session source has been closed.",
                (await Assert.ThrowsAsync<ObjectDisposedException>(() => waitingSessionTasks[i])).Message);
        }

        Assert.StartsWith("The session source has been closed.",
            (await Assert.ThrowsAsync<ObjectDisposedException>(async () => await sessionSource.OpenSession())).Message);
    }

    [Fact]
    public async Task StressTest_DisposeAsync_Close_Driver()
    {
        const int contentionTasks = 200;
        const int maxPoolSize = 100;
        for (var it = 0; it < 100_000; it++)
        {
            var disposeCalled = false;
            var mockFactory = new MockPoolingSessionFactory(maxPoolSize)
            {
                Dispose = () =>
                {
                    Volatile.Write(ref disposeCalled, true);
                    return ValueTask.CompletedTask;
                }
            };
            var settings = new YdbConnectionStringBuilder { MaxPoolSize = maxPoolSize };
            var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
            var openSessionTasks = new List<Task>();
            for (var i = 0; i < contentionTasks; i++)
            {
                var itCopy = it;
                var iCopy = i;

                openSessionTasks.Add(Task.Run(async () =>
                {
                    if (itCopy % contentionTasks == iCopy)
                    {
                        await sessionSource.DisposeAsync();
                        return;
                    }

                    try
                    {
                        using var session = await sessionSource.OpenSession();
                        await Task.Yield();
                        Assert.False(disposeCalled);
                    }
                    catch (ObjectDisposedException e)
                    {
                        Assert.StartsWith("The session source has been closed.", e.Message);
                    }
                    catch (OperationCanceledException)
                    {
                    }
                }));
            }

            await Task.WhenAll(openSessionTasks);
            Assert.Equal(0, mockFactory.NumSession);
            Assert.True(disposeCalled);
        }
    }

    [Fact]
    public async Task DisposeAsync_WhenSessionIsLeaked_ThrowsYdbExceptionWithTimeoutMessage()
    {
        var disposeCalled = false;
        const int maxPoolSize = 10;
        var mockFactory = new MockPoolingSessionFactory(maxPoolSize)
        {
            Dispose = () =>
            {
                Volatile.Write(ref disposeCalled, true);
                return ValueTask.CompletedTask;
            }
        };
        var settings = new YdbConnectionStringBuilder { MaxPoolSize = maxPoolSize };
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);

#pragma warning disable CA2012
        _ = sessionSource.OpenSession(CancellationToken.None);
#pragma warning restore CA2012

        Assert.Equal("Timeout while disposing of the pool: some sessions are still active. " +
                     "This may indicate a connection leak or suspended operations.",
            (await Assert.ThrowsAsync<YdbException>(async () => await sessionSource.DisposeAsync())).Message);
        Assert.True(disposeCalled);
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            sessionSource.OpenSession(CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task IdleTimeout_MinPoolSize_CloseNumSessionsMinusMinSessionCount()
    {
        const int maxPoolSize = 50;
        const int minPoolSize = 10;
        const int idleTimeoutSeconds = 1;

        var mockFactory = new MockPoolingSessionFactory(maxPoolSize);
        var settings = new YdbConnectionStringBuilder
        {
            SessionIdleTimeout = idleTimeoutSeconds,
            MaxPoolSize = maxPoolSize,
            MinPoolSize = minPoolSize
        };
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);

        var openSessions = new List<ISession>();
        for (var it = 0; it < maxPoolSize; it++)
        {
            openSessions.Add(await sessionSource.OpenSession());
        }

        foreach (var it in openSessions)
        {
            it.Dispose();
        }

        await Task.Delay(TimeSpan.FromSeconds(idleTimeoutSeconds * 5)); // cleaning idle sessions
        Assert.Equal(minPoolSize, mockFactory.NumSession);

        var openSessionTasks = new List<Task<ISession>>();
        for (var it = 0; it < minPoolSize; it++)
        {
            openSessionTasks.Add(Task.Run(async () => await sessionSource.OpenSession()));
        }

        foreach (var it in openSessionTasks)
        {
            (await it).Dispose();
        }

        Assert.Equal(minPoolSize, mockFactory.NumSession);
        Assert.Equal(maxPoolSize, mockFactory.SessionOpenedCount);
    }

    [Fact]
    public async Task StressTest_HighContention_OpenClose()
    {
        var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMinutes(1));

        const int maxPoolSize = 50;
        const int minPoolSize = 10;
        const int highContentionTasks = maxPoolSize * 5;
        var sessionIdIsBroken = new ConcurrentDictionary<int, bool>();

        var mockFactory = new MockPoolingSessionFactory(maxPoolSize)
        {
            IsBroken = sessionNum =>
            {
                var isBroken = Random.Shared.NextDouble() < 0.2;
                sessionIdIsBroken[sessionNum] = isBroken;
                return isBroken;
            },
            Open = sessionNum =>
            {
                sessionIdIsBroken[sessionNum] = false;
                return Task.CompletedTask;
            }
        };
        var settings = new YdbConnectionStringBuilder
            { MaxPoolSize = maxPoolSize, MinPoolSize = minPoolSize };
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);

        var workers = new List<Task>();
        for (var it = 0; it < highContentionTasks; it++)
        {
            workers.Add(Task.Run(async () =>
            {
                try
                {
                    while (!cts.IsCancellationRequested)
                    {
                        using (var session = await sessionSource.OpenSession(cts.Token))
                        {
                            Assert.False(sessionIdIsBroken[session.SessionId()]);
                        }

                        await Task.Delay(Random.Shared.Next(maxPoolSize), cts.Token);
                    }
                }
                catch (OperationCanceledException)
                {
                }
                catch (YdbException)
                {
                }
            }, cts.Token));
        }

        await Task.WhenAll(workers);
    }

    [Fact]
    public async Task Get_Session_From_Exhausted_Pool()
    {
        var mockFactory = new MockPoolingSessionFactory(1);
        var settings = new YdbConnectionStringBuilder
        {
            MaxPoolSize = 1,
            MinPoolSize = 0
        };

        var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);
        using var session = await sessionSource.OpenSession();
        var cts = new CancellationTokenSource();
        cts.CancelAfter(500);

        Assert.Equal("The connection pool has been exhausted, either raise 'MaxPoolSize' (currently 1) " +
                     "or 'CreateSessionTimeout' (currently 5 seconds) in your connection string.",
            (await Assert.ThrowsAsync<YdbException>(async () => await sessionSource.OpenSession(cts.Token))).Message);

        Assert.Equal(1, mockFactory.NumSession);
        Assert.Equal(1, mockFactory.SessionOpenedCount);
    }

    [Fact]
    public async Task ReturnToPool_WhenSessionIsBroken_IsSkipped()
    {
        const int maxPoolSize = 10;
        var mockFactory = new MockPoolingSessionFactory(maxPoolSize) { IsBroken = _ => true };
        var settings = new YdbConnectionStringBuilder
        {
            MaxPoolSize = maxPoolSize,
            MinPoolSize = 0
        };
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);

        for (var it = 0; it < maxPoolSize * 2; it++)
        {
            using var session = await sessionSource.OpenSession();
        }

        Assert.Equal(0, mockFactory.NumSession);
        Assert.Equal(maxPoolSize * 2, mockFactory.SessionOpenedCount);
    }

    [Fact]
    public async Task CheckIdleSession_WhenIsBrokenInStack_CreateNewSession()
    {
        var isBroken = false;
        const int maxPoolSize = 10;
        // ReSharper disable once AccessToModifiedClosure
        var mockFactory = new MockPoolingSessionFactory(maxPoolSize) { IsBroken = _ => isBroken };
        var settings = new YdbConnectionStringBuilder
        {
            MaxPoolSize = maxPoolSize,
            MinPoolSize = 0
        };
        var sessionSource = new PoolingSessionSource<MockPoolingSession>(mockFactory, settings);

        var openSessions = new List<ISession>();
        for (var it = 0; it < maxPoolSize; it++)
        {
            openSessions.Add(await sessionSource.OpenSession());
        }

        foreach (var session in openSessions)
        {
            session.Dispose();
        }

        Assert.Equal(maxPoolSize, mockFactory.NumSession);

        isBroken = true;
        for (var it = 0; it < maxPoolSize; it++)
        {
            using var session = await sessionSource.OpenSession();
            isBroken = false;
        }

        Assert.Equal(1, mockFactory.NumSession);
        Assert.Equal(maxPoolSize + 1, mockFactory.SessionOpenedCount);
    }
}
