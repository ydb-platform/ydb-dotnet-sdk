using System.Security.Cryptography.X509Certificates;
using EntityFrameworkCore.Ydb.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Ydb.Sdk.Auth;

namespace EntityFrameworkCore.Ydb.Infrastructure;

public class YdbDbContextOptionsBuilder(DbContextOptionsBuilder optionsBuilder)
    : RelationalDbContextOptionsBuilder<YdbDbContextOptionsBuilder, YdbOptionsExtension>(optionsBuilder)
{
    public YdbDbContextOptionsBuilder WithCredentialsProvider(ICredentialsProvider? credentialsProvider) =>
        WithOption(optionsBuilder => optionsBuilder.WithCredentialsProvider(credentialsProvider));

    public YdbDbContextOptionsBuilder WithServerCertificates(X509Certificate2Collection? serverCertificates) =>
        WithOption(optionsBuilder => optionsBuilder.WithServerCertificates(serverCertificates));
}
