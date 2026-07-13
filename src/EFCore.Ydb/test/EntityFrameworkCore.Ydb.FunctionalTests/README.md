# EFCore.Ydb Functional Tests

This directory contains functional tests for the EFCore.Ydb provider, based on the EF Core specification test suite.

## Overview

The test suite validates the EFCore.Ydb provider against the standard EF Core functional test patterns. These tests help ensure:
- Compatibility with EF Core APIs
- Correct translation of LINQ queries to YQL
- Proper handling of data types, relationships, and schema operations
- Identification of YDB-specific limitations and behaviors

## Test Organization

### Existing Test Coverage

The test suite currently includes tests for:

**Core Features:**
- Complex type tracking (`ComplexTypesTrackingYdbTest`)
- Concurrency detection (`ConcurrencyDetectorYdbTest`)
- Data binding (`DataBindingYdbTest`)
- Entity finding (`FindYdbTest`)
- Loading patterns (`LoadYdbTest`)
- Field mapping (`FieldMappingYdbTest`)
- Property values (`PropertyValuesYdbTest`)
- Transaction handling (`TransactionYdbTest`)

**Relationships:**
- Many-to-many loading and tracking (`ManyToManyLoadYdbTest`, `ManyToManyTrackingYdbTest`)
- One-to-one includes (`IncludeOneToOneYdbTest`)

**Query Features:**
- Northwind queries (aggregate, group by, compiled queries)
- Gears of War queries (TPH, TPT, TPC inheritance)
- Complex navigations
- Entity splitting
- Null key handling
- Ad-hoc query scenarios

**Bulk Operations:**
- Bulk updates for various inheritance strategies
- Complex type bulk updates
- Northwind bulk updates

**Migrations & Schema:**
- Migration SQL generation (`YdbMigrationsSqlGeneratorTest`)
- Migration infrastructure (`YdbMigrationsInfrastructureTest`)
- Model building (`YdbModelBuilderGenericTest`)

**Interception:**
- Command interception (`CommandInterceptionYdbTest`)
- SaveChanges interception (`SaveChangesInterceptionYdbTest`)
- Query expression interception (`QueryExpressionInterceptionYdbTest`)
- Transaction interception (`TransactionInterceptionYdbTest`)
- Materialization interception (`MaterializationInterceptionYdbTest`)

## Adding New Tests

To add a new test class based on EF Core specification tests:

### 1. Identify the Test Base Class

Find the appropriate test base class from `Microsoft.EntityFrameworkCore.Relational.Specification.Tests`:
```csharp
// Example: Adding a test for owned entity types
public class MyNewYdbTest : MyTestBase<MyNewYdbTest.MyNewYdbFixture>
{
    // ...
}
```

### 2. Create the Fixture

Implement the fixture using `YdbTestStoreFactory`:
```csharp
public class MyNewYdbFixture : MyFixtureBase
{
    protected override string StoreName => "MyTestDatabase";
    
    protected override ITestStoreFactory TestStoreFactory 
        => YdbTestStoreFactory.Instance;
    
    protected override void OnModelCreating(ModelBuilder modelBuilder, DbContext context)
    {
        base.OnModelCreating(modelBuilder, context);
        // YDB-specific configuration
    }
}
```

### 3. Skip Incompatible Tests

For tests that cannot pass due to YDB limitations, override the test method and return `Task.CompletedTask` with a clear skip reason:

```csharp
public override Task Some_test_method(bool async)
    => Task.CompletedTask; // Skip: YDB server limitation - reason here
```

### 4. Categorize Skip Reasons

Use consistent skip reasons to categorize limitations:

- **YDB server limitation** - Feature not supported by YDB/YQL engine
  ```csharp
  => Task.CompletedTask; // Skip: YDB server limitation - savepoint semantics differ
  ```

- **Provider implementation gap** - Feature could be implemented in provider
  ```csharp
  => Task.CompletedTask; // Skip: YDB provider implementation - query translation pending
  ```

### 5. Document in YDB_LIMITATIONS.md

Add the limitation to `YDB_LIMITATIONS.md` to track known issues systematically.

## Running Tests

### Run All Tests
```bash
dotnet test
```

### Run Specific Test Class
```bash
dotnet test --filter "FullyQualifiedName~ManyToManyLoadYdbTest"
```

### Run Tests Matching Pattern
```bash
dotnet test --filter "FullyQualifiedName~Query"
```

## Understanding Test Failures

When tests fail:

1. **Check if it's a known limitation** - See `YDB_LIMITATIONS.md`
2. **Verify YDB server support** - Consult YQL documentation
3. **Check provider translation** - Enable sensitive data logging to see generated YQL
4. **File an issue** - If it's a new issue, document it

## Test Infrastructure

### Key Files
- `TestUtilities/YdbTestStore.cs` - Test database lifecycle management
- `TestUtilities/YdbTestStoreFactory.cs` - Factory for creating test stores
- `YdbFixture.cs` - Base fixture for YDB tests
- `xunit.runner.json` - xUnit configuration

### Northwind Test Data
- `Northwind.sql` - Test data for Northwind-based queries
- Loaded via `YdbNorthwindTestStoreFactory`

## Contributing

When adding or modifying tests:
1. Follow existing patterns and naming conventions
2. Add XML documentation explaining test purpose
3. Clearly mark and document any skipped tests
4. Keep YDB_LIMITATIONS.md updated
5. Ensure tests build successfully before committing

## References

- [EF Core Testing Documentation](https://learn.microsoft.com/en-us/ef/core/testing/)
- [YDB Documentation](https://ydb.tech/docs)
- [YQL Reference](https://ydb.tech/docs/yql/reference/)
