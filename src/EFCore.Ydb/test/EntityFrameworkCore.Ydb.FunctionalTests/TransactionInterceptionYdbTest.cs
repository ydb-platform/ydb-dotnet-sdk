using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public abstract class TransactionInterceptionYdbTestBase(
    TransactionInterceptionYdbTestBase.InterceptionYdbFixtureBase fixture
)
    : TransactionInterceptionTestBase(fixture)
{
    public abstract class InterceptionYdbFixtureBase : InterceptionFixtureBase
    {
        protected override string StoreName
            => "TransactionInterception";

        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        protected override IServiceCollection InjectInterceptors(
            IServiceCollection serviceCollection,
            IEnumerable<IInterceptor> injectedInterceptors
        )
            => base.InjectInterceptors(serviceCollection.AddEntityFrameworkYdb(), injectedInterceptors);
    }

    public class TransactionInterceptionYdbTest(
        TransactionInterceptionYdbTest.InterceptionYdbFixture fixture
    ) : TransactionInterceptionYdbTestBase(fixture),
        IClassFixture<TransactionInterceptionYdbTest.InterceptionYdbFixture>
    {
        [ConditionalTheory(Skip = "Unsupported isolation level")]
        [InlineData(false)]
        [InlineData(true)]
        public override Task Intercept_BeginTransaction_with_isolation_level(bool async)
            => base.Intercept_BeginTransaction_with_isolation_level(async);

        // Savepoints are not supported in YDB
        public override Task Intercept_CreateSavepoint(bool async) => Task.CompletedTask;
        public override Task Intercept_ReleaseSavepoint(bool async) => Task.CompletedTask;
        public override Task Intercept_RollbackToSavepoint(bool async) => Task.CompletedTask;

        public class InterceptionYdbFixture : InterceptionYdbFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener => false;
        }
    }

    public class TransactionInterceptionWithDiagnosticsYdbTest(
        TransactionInterceptionWithDiagnosticsYdbTest.InterceptionYdbFixture fixture
    ) : TransactionInterceptionYdbTestBase(fixture),
        IClassFixture<TransactionInterceptionWithDiagnosticsYdbTest.InterceptionYdbFixture>
    {
        [ConditionalTheory(Skip = "Unsupported isolation level")]
        [InlineData(false)]
        [InlineData(true)]
        public override Task Intercept_BeginTransaction_with_isolation_level(bool async)
            => base.Intercept_BeginTransaction_with_isolation_level(async);


        // Savepoints are not supported in YDB
        public override Task Intercept_CreateSavepoint(bool async) => Task.CompletedTask;
        public override Task Intercept_ReleaseSavepoint(bool async) => Task.CompletedTask;
        public override Task Intercept_RollbackToSavepoint(bool async) => Task.CompletedTask;

        public class InterceptionYdbFixture : InterceptionYdbFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener => true;
        }
    }
}
