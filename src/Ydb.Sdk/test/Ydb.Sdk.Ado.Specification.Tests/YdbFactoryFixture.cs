using System.Data.Common;
using AdoNet.Specification.Tests;

namespace Ydb.Sdk.Ado.Specification.Tests;

public class YdbFactoryFixture : IDbFactoryFixture
{
    public DbProviderFactory Factory => YdbProviderFactory.Instance;

    public string ConnectionString => "Host=localhost;Port=2136;Database=local;MaxPoolSize=10";
}
