using AdoNet.Specification.Tests;

namespace Ydb.Sdk.Ado.Specification.Tests;

public class YdbConnectionStringBuilderTests(YdbFactoryFixture fixture)
    : ConnectionStringTestBase<YdbFactoryFixture>(fixture);
