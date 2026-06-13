using System.Security.Cryptography.X509Certificates;
using EntityFrameworkCore.Ydb.Infrastructure.Internal;
using EntityFrameworkCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado.RetryPolicy;
using Ydb.Sdk.Auth;

namespace EntityFrameworkCore.Ydb.Infrastructure;

public sealed class YdbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    : RelationalDbContextOptionsBuilder<YdbDbContextOptionsBuilder, YdbOptionsExtension>(optionsBuilder)
{
    public YdbDbContextOptionsBuilder UseCredentialsProvider(ICredentialsProvider? credentialsProvider) =>
        WithOption(optionsBuilder => optionsBuilder.WithCredentialsProvider(credentialsProvider));

    public YdbDbContextOptionsBuilder UseServerCertificates(X509Certificate2Collection? serverCertificates) =>
        WithOption(optionsBuilder => optionsBuilder.WithServerCertificates(serverCertificates));

    /// <summary>
    /// Configures the EF Core execution strategy to retry idempotent operations.
    /// Equivalent to <see cref="UseRetryPolicy"/> with
    /// <see cref="YdbRetryPolicyConfig.EnableRetryIdempotence"/> set to <c>true</c>;
    /// other settings stay at <see cref="YdbRetryPolicyConfig"/> defaults.
    /// </summary>
    public YdbDbContextOptionsBuilder EnableRetryIdempotence() =>
        UseRetryPolicy(new YdbRetryPolicyConfig { EnableRetryIdempotence = true });

    /// <summary>
    /// Configures the EF Core execution strategy to use a <see cref="YdbRetryPolicy"/> built from
    /// the supplied <paramref name="retryPolicyConfig"/> (max attempts, backoff bases / caps,
    /// idempotence).
    /// </summary>
    /// <param name="retryPolicyConfig">Tuning knobs for <see cref="YdbRetryPolicy"/>.</param>
    public YdbDbContextOptionsBuilder UseRetryPolicy(YdbRetryPolicyConfig retryPolicyConfig) =>
        ExecutionStrategy(d => new YdbExecutionStrategy(d, retryPolicyConfig));

    /// <summary>
    /// Disables retries entirely: the strategy will surface the first failure to the caller.
    /// </summary>
    public YdbDbContextOptionsBuilder DisableRetryOnFailure() =>
        ExecutionStrategy(d => new NonRetryingExecutionStrategy(d));
}
