using AdoNet.Specification.Tests;

namespace Ydb.Sdk.Ado.Specification.Tests;

public class YdbTransactionTests(YdbFactoryFixture fixture) : TransactionTestBase<YdbFactoryFixture>(fixture);
