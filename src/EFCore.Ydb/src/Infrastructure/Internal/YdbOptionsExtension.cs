using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Ydb.Sdk.Auth;

namespace EntityFrameworkCore.Ydb.Infrastructure.Internal;

public class YdbOptionsExtension : RelationalOptionsExtension
{
    public ICredentialsProvider? CredentialsProvider { get; private set; }

    public X509Certificate2Collection? ServerCertificates { get; private set; }

    public bool DisableRetryExecutionStrategy { get; private set; }

    private DbContextOptionsExtensionInfo? _info;

    public YdbOptionsExtension()
    {
    }

    private YdbOptionsExtension(YdbOptionsExtension copyFrom) : base(copyFrom)
    {
        CredentialsProvider = copyFrom.CredentialsProvider;
        ServerCertificates = copyFrom.ServerCertificates;
        DisableRetryExecutionStrategy = copyFrom.DisableRetryExecutionStrategy;
    }

    protected override RelationalOptionsExtension Clone() => new YdbOptionsExtension(this);

    public override void ApplyServices(IServiceCollection services) =>
        services.AddEntityFrameworkYdb(!DisableRetryExecutionStrategy);

    public override DbContextOptionsExtensionInfo Info => _info ??= new ExtensionInfo(this);

    public YdbOptionsExtension WithCredentialsProvider(ICredentialsProvider? credentialsProvider)
    {
        var clone = (YdbOptionsExtension)Clone();

        clone.CredentialsProvider = credentialsProvider;

        return clone;
    }

    public YdbOptionsExtension WithServerCertificates(X509Certificate2Collection? serverCertificates)
    {
        var clone = (YdbOptionsExtension)Clone();

        clone.ServerCertificates = serverCertificates;

        return clone;
    }

    public YdbOptionsExtension DisableRetryOnFailure()
    {
        var clone = (YdbOptionsExtension)Clone();

        clone.DisableRetryExecutionStrategy = true;

        return clone;
    }

    private sealed class ExtensionInfo(YdbOptionsExtension extension) : RelationalExtensionInfo(extension)
    {
        public override bool IsDatabaseProvider => true;

        public override void PopulateDebugInfo(IDictionary<string, string> debugInfo)
        {
        }
    }
}
