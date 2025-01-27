using System.Data.Common;
using AdoNet.Specification.Tests;
using Ydb.Sdk.Ado;

namespace Ydb.Sdk.Tests.Ado.Specification;

public class YdbFactoryFixture : IDbFactoryFixture
{
    public DbProviderFactory Factory => YdbProviderFactory.Instance;

    public string ConnectionString => "Host=localhost;Port=2136;Database=/local";
}
