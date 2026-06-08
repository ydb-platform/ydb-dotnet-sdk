using Microsoft.Extensions.Logging;
using Ydb.Coordination;
using Ydb.Coordination.V1;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Coordination.Description;
using Ydb.Sdk.Coordination.Recipes;
using static Ydb.Sdk.Ado.PoolManager;

namespace Ydb.Sdk.Coordination;

/// <summary>
/// Client entry point for the YDB Coordination Service.
/// </summary>
/// <remarks>
/// <para>
/// <see cref="CoordinationClient"/> manages a shared <see cref="IDriver"/> (pooled per connection
/// string) and exposes two layers of API:
/// </para>
/// <list type="bullet">
///   <item>
///     <term>Node management</term>
///     <description>
///       <see cref="CreateNodeAsync"/>, <see cref="AlterNodeAsync"/>, <see cref="DropNodeAsync"/>,
///       <see cref="DescribeNodeAsync"/> — unary calls against the configuration of a coordination node.
///     </description>
///   </item>
///   <item>
///     <term>Recipes</term>
///     <description>
///       High-level primitives built on top of the underlying semaphore protocol — each
///       method below opens its own <see cref="CoordinationSession"/> for the lifetime
///       of the returned handle. The handle is <see cref="IAsyncDisposable"/>; disposing it
///       releases the lock / resigns leadership / closes the underlying session.
///       <list type="bullet">
///         <item><see cref="AcquireLockAsync"/> / <see cref="TryAcquireLockAsync"/></item>
///         <item><see cref="CampaignAsync"/> / <see cref="ObserveLeaderAsync"/></item>
///         <item><see cref="RegisterServiceAsync"/> / <see cref="DiscoverServiceAsync"/></item>
///         <item><see cref="PublishConfigAsync"/> / <see cref="SubscribeConfigAsync"/></item>
///       </list>
///     </description>
///   </item>
/// </list>
/// </remarks>
public sealed class CoordinationClient : IAsyncDisposable
{
    private readonly IDriver _driver;
    private readonly ILogger<CoordinationClient> _logger;
    private int _disposed;

    public CoordinationClient(string connectionString)
        : this(GetDriver(new YdbConnectionStringBuilder(connectionString)).AsTask().GetAwaiter().GetResult())
    {
    }

    public CoordinationClient(YdbConnectionStringBuilder connectionStringBuilder)
        : this(GetDriver(connectionStringBuilder).AsTask().GetAwaiter().GetResult())
    {
    }

    internal CoordinationClient(IDriver driver)
    {
        _driver = driver ?? throw new ArgumentNullException(nameof(driver));
        _logger = driver.LoggerFactory.CreateLogger<CoordinationClient>();
    }

    internal IDriver Driver => _driver;

    // ------------------------------------------------------------------------------------------
    // Node management
    // ------------------------------------------------------------------------------------------

    public async Task CreateNodeAsync(string path, NodeConfig config, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Creating coordination node at {Path}", path);
        await _driver.UnaryCall(
            CoordinationService.CreateNodeMethod,
            new CreateNodeRequest { Path = ResolvePath(path), Config = config.ToProto() },
            new GrpcRequestSettings { CancellationToken = cancellationToken }).ConfigureAwait(false);
    }

    public async Task AlterNodeAsync(string path, NodeConfig config, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Altering coordination node at {Path}", path);
        await _driver.UnaryCall(
            CoordinationService.AlterNodeMethod,
            new AlterNodeRequest { Path = ResolvePath(path), Config = config.ToProto() },
            new GrpcRequestSettings { CancellationToken = cancellationToken }).ConfigureAwait(false);
    }

    public async Task DropNodeAsync(string path, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Dropping coordination node at {Path}", path);
        await _driver.UnaryCall(
            CoordinationService.DropNodeMethod,
            new DropNodeRequest { Path = ResolvePath(path) },
            new GrpcRequestSettings { CancellationToken = cancellationToken }).ConfigureAwait(false);
    }

    public async Task<NodeConfig> DescribeNodeAsync(string path, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Describing coordination node at {Path}", path);
        var response = await _driver.UnaryCall(
            CoordinationService.DescribeNodeMethod,
            new DescribeNodeRequest { Path = ResolvePath(path) },
            new GrpcRequestSettings { CancellationToken = cancellationToken }).ConfigureAwait(false);
        return NodeConfig.FromProto(response.Operation.Result.Unpack<DescribeNodeResult>());
    }

    // ------------------------------------------------------------------------------------------
    // Low-level session
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Opens a new <see cref="CoordinationSession"/> attached to <paramref name="nodePath"/>.
    /// </summary>
    /// <remarks>The session is returned after the initial <c>SessionStarted</c> reply has been observed.</remarks>
    public async Task<CoordinationSession> OpenSessionAsync(
        string nodePath,
        CoordinationSessionOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var session = new CoordinationSession(_driver, ResolvePath(nodePath), options);
        try
        {
            await session.WaitReadyAsync(cancellationToken).ConfigureAwait(false);
            return session;
        }
        catch
        {
            await session.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    // ------------------------------------------------------------------------------------------
    // Recipes
    // ------------------------------------------------------------------------------------------

    /// <summary>
    /// Acquires a named distributed lock. Blocks until the lock is granted, the wait
    /// times out, or <paramref name="cancellationToken"/> is signalled.
    /// </summary>
    public Task<DistributedLock> AcquireLockAsync(
        string nodePath,
        string lockName,
        byte[]? data = null,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
        => DistributedLock.AcquireAsync(this, nodePath, lockName, data, timeout, cancellationToken);

    /// <summary>
    /// Attempts to acquire a lock without waiting. Returns <c>null</c> if the lock is currently held.
    /// </summary>
    public Task<DistributedLock?> TryAcquireLockAsync(
        string nodePath,
        string lockName,
        byte[]? data = null,
        CancellationToken cancellationToken = default)
        => DistributedLock.TryAcquireAsync(this, nodePath, lockName, data, cancellationToken);

    /// <summary>
    /// Joins a named leader election. Returns once this candidate has been elected.
    /// </summary>
    public Task<Leadership> CampaignAsync(
        string nodePath,
        string electionName,
        byte[] data,
        CancellationToken cancellationToken = default)
        => Leadership.CampaignAsync(this, nodePath, electionName, data, cancellationToken);

    /// <summary>
    /// Subscribes to leader changes without participating in the election.
    /// </summary>
    public Task<LeaderObserver> ObserveLeaderAsync(
        string nodePath,
        string electionName,
        CancellationToken cancellationToken = default)
        => LeaderObserver.OpenAsync(this, nodePath, electionName, cancellationToken);

    /// <summary>
    /// Registers this process as a participant of a named service, publishing
    /// <paramref name="endpoint"/> as its directory entry.
    /// </summary>
    public Task<ServiceRegistration> RegisterServiceAsync(
        string nodePath,
        string serviceName,
        byte[] endpoint,
        CancellationToken cancellationToken = default)
        => ServiceRegistration.RegisterAsync(this, nodePath, serviceName, endpoint, cancellationToken);

    /// <summary>
    /// Subscribes to the membership of a named service.
    /// </summary>
    public Task<ServiceDiscovery> DiscoverServiceAsync(
        string nodePath,
        string serviceName,
        CancellationToken cancellationToken = default)
        => ServiceDiscovery.OpenAsync(this, nodePath, serviceName, cancellationToken);

    /// <summary>
    /// Becomes the exclusive publisher of a named configuration entry.
    /// </summary>
    public Task<ConfigPublisher> PublishConfigAsync(
        string nodePath,
        string configName,
        byte[] initialValue,
        CancellationToken cancellationToken = default)
        => ConfigPublisher.OpenAsync(this, nodePath, configName, initialValue, cancellationToken);

    /// <summary>
    /// Subscribes to a named configuration entry.
    /// </summary>
    public Task<ConfigSubscription> SubscribeConfigAsync(
        string nodePath,
        string configName,
        CancellationToken cancellationToken = default)
        => ConfigSubscription.OpenAsync(this, nodePath, configName, cancellationToken);

    // ------------------------------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------------------------------

    internal string ResolvePath(string path)
    {
        if (string.IsNullOrEmpty(path))
            throw new ArgumentException("Coordination node path cannot be empty", nameof(path));

        return path.StartsWith('/') ? path : $"{_driver.Database}/{path}";
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await _driver.DisposeAsync().ConfigureAwait(false);
        GC.SuppressFinalize(this);
    }
}
