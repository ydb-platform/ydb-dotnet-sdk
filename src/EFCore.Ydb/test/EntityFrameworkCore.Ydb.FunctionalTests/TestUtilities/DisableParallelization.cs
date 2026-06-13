using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;

[CollectionDefinition("DisableParallelization", DisableParallelization = true)]
public class DisableParallelization;
