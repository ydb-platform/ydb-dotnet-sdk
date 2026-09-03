using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.TestUtilities;
using Microsoft.Extensions.DependencyInjection;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

/// <summary>
/// Tests for command interception in YDB provider.
/// Note: Some interception tests are skipped due to YDB-specific implementation differences.
/// </summary>
public abstract class CommandInterceptionYdbTestBase : CommandInterceptionTestBase
{
    protected CommandInterceptionYdbTestBase(InterceptionYdbFixtureBase fixture)
        : base(fixture)
    {
    }

    public abstract class InterceptionYdbFixtureBase : InterceptionFixtureBase
    {
        protected override string StoreName => "CommandInterception";

        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

        protected override IServiceCollection InjectInterceptors(
            IServiceCollection serviceCollection,
            IEnumerable<IInterceptor> injectedInterceptors)
            => base.InjectInterceptors(serviceCollection.AddEntityFrameworkYdb(), injectedInterceptors);
    }

    public class CommandInterceptionYdbTest(CommandInterceptionYdbTest.InterceptionYdbFixture fixture)
        : CommandInterceptionYdbTestBase(fixture)
    {
        public class InterceptionYdbFixture : InterceptionYdbFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener => false;
        }
    }

    public class CommandInterceptionWithDiagnosticsYdbTest(
        CommandInterceptionWithDiagnosticsYdbTest.InterceptionYdbFixture fixture)
        : CommandInterceptionYdbTestBase(fixture)
    {
        public class InterceptionYdbFixture : InterceptionYdbFixtureBase
        {
            protected override bool ShouldSubscribeToDiagnosticListener => true;
        }
    }
}
