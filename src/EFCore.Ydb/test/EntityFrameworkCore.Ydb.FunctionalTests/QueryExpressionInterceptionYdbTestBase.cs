using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using EntityFrameworkCore.Ydb.Infrastructure;
using EntityFrameworkCore.Ydb.Storage.Internal;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public abstract class QueryExpressionInterceptionYdbTestBase(
    QueryExpressionInterceptionYdbTestBase.InterceptionYdbFixtureBase fixture)
    : QueryExpressionInterceptionTestBase(fixture)
{
    public abstract class InterceptionYdbFixtureBase : InterceptionFixtureBase
    {
        protected override ITestStoreFactory TestStoreFactory
            => YdbTestStoreFactory.Instance;

        protected override IServiceCollection InjectInterceptors(
            IServiceCollection serviceCollection,
            IEnumerable<IInterceptor> injectedInterceptors)
            => base.InjectInterceptors(serviceCollection.AddEntityFrameworkYdb(), injectedInterceptors);

        public override DbContextOptionsBuilder AddOptions(DbContextOptionsBuilder builder)
        {
            new YdbDbContextOptionsBuilder(base.AddOptions(builder))
                .ExecutionStrategy(d => new YdbExecutionStrategy(d));
            return builder;
        }
    }

    public class QueryExpressionInterceptionYdbTest(QueryExpressionInterceptionYdbTest.InterceptionYdbFixture fixture)
        : QueryExpressionInterceptionYdbTestBase(fixture),
            IClassFixture<QueryExpressionInterceptionYdbTest.InterceptionYdbFixture>
    {
        public class InterceptionYdbFixture : InterceptionYdbFixtureBase
        {
            protected override string StoreName
                => "QueryExpressionInterception";

            protected override bool ShouldSubscribeToDiagnosticListener
                => false;
        }
    }

    public class QueryExpressionInterceptionWithDiagnosticsYdbTest(
        QueryExpressionInterceptionWithDiagnosticsYdbTest.InterceptionYdbFixture fixture)
        : QueryExpressionInterceptionYdbTestBase(fixture),
            IClassFixture<QueryExpressionInterceptionWithDiagnosticsYdbTest.InterceptionYdbFixture>
    {
        public class InterceptionYdbFixture : InterceptionYdbFixtureBase
        {
            protected override string StoreName
                => "QueryExpressionInterceptionWithDiagnostics";

            protected override bool ShouldSubscribeToDiagnosticListener
                => true;
        }
    }
}
