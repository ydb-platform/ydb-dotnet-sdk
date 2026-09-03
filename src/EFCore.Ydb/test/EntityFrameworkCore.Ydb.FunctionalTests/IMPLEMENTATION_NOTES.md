# Implementation Notes for EFCore.Ydb Functional Tests

This document provides implementation notes and context for developers working on the EFCore.Ydb functional test suite.

## Project Overview

This test project validates the EFCore.Ydb provider against the standard EF Core functional test specifications from `Microsoft.EntityFrameworkCore.Relational.Specification.Tests`.

### Goals
1. **Maximize Test Coverage** - Run as many EF Core specification tests as possible
2. **Document Limitations** - Clearly identify and categorize limitations (server vs provider)
3. **Enable Debugging** - Provide clear skip reasons for tests that cannot pass
4. **Guide Contributors** - Establish patterns for adding new tests

## Architecture

### Test Organization

```
EntityFrameworkCore.Ydb.FunctionalTests/
├── README.md                          # User guide for tests
├── YDB_LIMITATIONS.md                 # Systematic limitation catalog
├── IMPLEMENTATION_NOTES.md            # This file (developer guide)
│
├── Query/                             # Query-related tests
│   ├── Northwind*.cs                  # Northwind database queries
│   ├── GearsOfWar*.cs                 # Complex inheritance queries
│   ├── ComplexNavigations*.cs         # Navigation property tests
│   └── AdHoc*.cs                      # Ad-hoc query scenarios
│
├── BulkUpdates/                       # Bulk operation tests
├── Migrations/                        # Migration tests
├── ModelBuilding/                     # Model configuration tests
├── Update/                            # Update operation tests
├── TestUtilities/                     # Shared test infrastructure
│
└── [Test Classes]                     # Core functional tests
```

### Test Infrastructure

Key infrastructure components:

1. **YdbTestStore** - Manages test database lifecycle
2. **YdbTestStoreFactory** - Creates test stores for fixtures
3. **YdbFixture** - Base fixture configuration
4. **YdbNorthwindTestStoreFactory** - Northwind-specific test setup

## Coding Patterns

### 1. Creating a New Test Class

```csharp
/// <summary>
/// Clear description of what this test validates.
/// Note: Any YDB-specific constraints or limitations.
/// </summary>
public class MyFeatureYdbTest : MyFeatureTestBase<MyFeatureYdbTest.MyFeatureYdbFixture>
{
    public MyFeatureYdbTest(MyFeatureYdbFixture fixture)
        : base(fixture)
    {
    }

    // Override and skip unsupported tests
    public override Task Unsupported_test(bool async)
        => Task.CompletedTask; // Skip: [YDB server limitation|YDB provider implementation] - reason

    public class MyFeatureYdbFixture : MyFeatureFixtureBase
    {
        protected override string StoreName => "MyFeatureTest";
        protected override ITestStoreFactory TestStoreFactory => YdbTestStoreFactory.Instance;

        protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
        {
            base.OnModelCreating(modelBuilder, context);
            // YDB-specific model configuration
        }
    }
}
```

### 2. Skip Reason Format

Use one of these standardized formats:

```csharp
// For YDB server/YQL engine limitations
=> Task.CompletedTask; // Skip: YDB server limitation - savepoint semantics differ

// For provider implementation gaps
=> Task.CompletedTask; // Skip: YDB provider implementation - query translation not implemented

// For known gaps under consideration
=> Task.CompletedTask; // Skip: Provider implementation gap - feature roadmap item
```

### 3. Primary Constructor Usage

The codebase uses C# 12 primary constructor syntax where appropriate:

```csharp
// ✅ Good - matches existing code style
public class MyTest(MyFixture fixture) : TestBase<MyFixture>(fixture)
{
}

// ⚠️ Avoid - inconsistent with codebase
public class MyTest : TestBase<MyFixture>
{
    public MyTest(MyFixture fixture) : base(fixture)
    {
    }
}
```

## Common Patterns and Solutions

### Pattern 1: Missing Test Base Class

**Problem**: Compilation error - test base class doesn't exist in EF Core 9.0

**Solution**: 
- Check if the test base exists: `grep "TestBase" existing_tests/*.cs`
- If it doesn't exist, the test may be deprecated or renamed
- Consider alternative test base classes or skip this test suite

### Pattern 2: Abstract Method Must Be Implemented

**Problem**: Compilation error - abstract property or method not implemented

**Solution**:
```csharp
public class MyFixture : BaseFixture
{
    // Implement required abstract members
    protected override string StoreName => "MyDatabase";
    protected override bool SnapshotSupported => false; // YDB limitation
}
```

### Pattern 3: Test Fails Due to YDB Limitation

**Problem**: Test fails consistently due to unsupported YDB feature

**Solution**:
1. Identify if it's a server or provider limitation
2. Add skip override with clear reason
3. Document in YDB_LIMITATIONS.md
4. Consider filing issue if it's a provider gap

### Pattern 4: Non-Virtual Test Method

**Problem**: Test fails but cannot be overridden (not virtual)

**Solution**:
- Document the limitation in class XML comment
- Use `#pragma warning disable xUnit1000` if necessary
- Explain constraint in YDB_LIMITATIONS.md

## YDB-Specific Considerations

### Connection Management

YDB uses session pooling rather than traditional connection pooling:
```csharp
// Connection strings point to YDB endpoints
protected override DbContext CreateContextWithConnectionString()
{
    var options = Fixture.AddOptions(
            new DbContextOptionsBuilder()
                .UseYdb(TestStore.ConnectionString))
        .UseInternalServiceProvider(Fixture.ServiceProvider);
    
    return new DbContext(options.Options);
}
```

### Transaction Handling

YDB transactions differ from traditional RDBMS:
- Savepoints have different semantics
- Nested transactions work differently
- Snapshot isolation behavior differs

Always test transaction-related code carefully and document limitations.

### Data Type Constraints

Key YDB type constraints to remember:
- Decimal: Limited to (22, 9) precision
- Time: No standalone Time type
- Binary: Constraints on binary keys
- Foreign Keys: Not enforced by database

### Query Translation

Common query translation gaps:
- Some correlated subqueries not supported
- Complex window function patterns
- Specific aggregate function combinations
- LIKE with escape character differences

## Debugging Tips

### Enable SQL Logging

```csharp
public MyTest(MyFixture fixture) : base(fixture)
{
    fixture.TestSqlLoggerFactory.Clear();
    fixture.TestSqlLoggerFactory.SetTestOutputHelper(outputHelper);
}
```

### Run Specific Test

```bash
dotnet test --filter "FullyQualifiedName~MyTest.MyMethod"
```

### Check Generated YQL

Look at test output to see generated YQL queries and identify translation issues.

## Contributing Workflow

1. **Identify Missing Tests** - Compare with other EF providers (e.g., Npgsql)
2. **Create Test Class** - Follow patterns in this document
3. **Run Tests** - Identify failures
4. **Categorize Failures** - Server limitation vs provider gap
5. **Add Skip Reasons** - Use standardized format
6. **Update Documentation** - Add to YDB_LIMITATIONS.md
7. **Submit PR** - Include build verification

## Performance Considerations

- Test execution speed depends on YDB instance
- Use test store caching where appropriate
- Avoid unnecessary database operations in fixtures
- Consider parallelization constraints

## Maintenance Notes

### Updating EF Core Version

When updating to new EF Core versions:
1. Update package reference in .csproj
2. Check for new/changed test base classes
3. Review breaking changes in EF Core
4. Update test implementations as needed
5. Re-run full test suite

### Adding YDB Features

When YDB adds new features:
1. Review skipped tests for newly supported patterns
2. Remove skip overrides where applicable
3. Update YDB_LIMITATIONS.md
4. Add tests for new features if not covered

## References

- [EF Core Docs](https://learn.microsoft.com/en-us/ef/core/)
- [YDB Docs](https://ydb.tech/docs)
- [YQL Reference](https://ydb.tech/docs/yql/reference/)
- [xUnit Docs](https://xunit.net/)

## Questions or Issues?

- Check existing test implementations for patterns
- Review YDB_LIMITATIONS.md for known constraints
- Consult YDB documentation for server capabilities
- File issues in GitHub for provider gaps
