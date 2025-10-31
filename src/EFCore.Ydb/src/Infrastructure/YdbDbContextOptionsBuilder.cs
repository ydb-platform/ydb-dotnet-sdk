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

    public YdbDbContextOptionsBuilder EnableRetryIdempotence()
        => UseRetryPolicy(new YdbRetryPolicyConfig { EnableRetryIdempotence = true });

    public YdbDbContextOptionsBuilder UseRetryPolicy(YdbRetryPolicyConfig retryPolicyConfig)
        => ExecutionStrategy(d => new YdbExecutionStrategy(d, retryPolicyConfig));

    public YdbDbContextOptionsBuilder DisableRetryOnFailure() =>
        ExecutionStrategy(d => new NonRetryingExecutionStrategy(d));
}
