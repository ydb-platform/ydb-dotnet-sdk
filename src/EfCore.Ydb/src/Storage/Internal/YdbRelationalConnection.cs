using System.Data.Common;
using System.Security.Cryptography.X509Certificates;
using EfCore.Ydb.Extensions;
using EfCore.Ydb.Infrastructure.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Auth;

namespace EfCore.Ydb.Storage.Internal;

public class YdbRelationalConnection : RelationalConnection, IYdbRelationalConnection
{
    private readonly ICredentialsProvider? _credentialsProvider;
    private readonly X509Certificate2Collection? _serverCertificates;

    public YdbRelationalConnection(RelationalConnectionDependencies dependencies) : base(dependencies)
    {
        var ydbOptionsExtension = dependencies.ContextOptions.FindExtension<YdbOptionsExtension>() ??
                                  new YdbOptionsExtension();

        _credentialsProvider = ydbOptionsExtension.CredentialsProvider;
        _serverCertificates = ydbOptionsExtension.ServerCertificates;
    }

    protected override DbConnection CreateDbConnection()
    {
        var ydbConnectionStringBuilder = new YdbConnectionStringBuilder(GetValidatedConnectionString())
        {
            CredentialsProvider = _credentialsProvider,
            ServerCertificates = _serverCertificates
        };

        return new YdbConnection(ydbConnectionStringBuilder);
    }

    public IYdbRelationalConnection Clone()
    {
        var connectionStringBuilder = new YdbConnectionStringBuilder(GetValidatedConnectionString());
        var options = new DbContextOptionsBuilder().UseYdb(connectionStringBuilder.ToString()).Options;
        return new YdbRelationalConnection(Dependencies with { ContextOptions = options });
    }
}
