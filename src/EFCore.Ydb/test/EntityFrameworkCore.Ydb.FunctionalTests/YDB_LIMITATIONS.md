# YDB Server and Provider Limitations

This document catalogues known limitations of the YDB database server (YQL) and the EFCore.Ydb provider implementation that affect EF Core functional tests.

## Categories

### 1. YDB Server Limitations (YQL/Engine)
These are fundamental limitations of the YDB database server that cannot be resolved purely in the provider.

#### Transaction & Concurrency
- **Savepoint semantics**: YDB's transaction savepoint implementation differs from traditional RDBMS
- **Nested transactions**: Limited support for nested transaction patterns
- **Snapshot isolation**: Snapshot-based isolation behavior differs
- **Transaction rollback**: Rollback semantics have YDB-specific constraints

#### Query Features
- **Correlated subqueries**: Some complex correlated subquery patterns not supported
- **Complex joins**: Certain multi-level join scenarios have limitations
- **Window functions**: Limited or different window function support
- **Recursive CTEs**: Common Table Expressions with recursion not fully supported
- **LIKE patterns**: Pattern matching with escape characters works differently
- **RANDOM() function**: Random number generation semantics differ from SQL standard

#### Data Types
- **Decimal precision**: Only supports (22, 9) precision instead of full decimal ranges
- **Time types**: Time-only data type support differs
- **Binary keys**: Specific constraints on using binary data as primary keys
- **Custom types**: Limited support for user-defined types

#### Schema Features
- **Foreign keys**: YDB does not enforce foreign key constraints at the database level
- **Computed columns**: Store-generated computed column behavior differs
- **Auto-increment/Identity**: Identity column semantics differ from traditional RDBMS
- **Indexes**: Filtered and partial index support has limitations
- **Collations**: Collation support differs from standard SQL databases

#### Data Manipulation
- **Bulk operations**: Certain bulk update/delete patterns not optimized
- **Complex updates**: Multi-table update operations have constraints
- **Collection queries**: Complex collection navigation queries may not be supported

### 2. Provider Implementation Gaps
These are limitations in the EFCore.Ydb provider that could potentially be addressed in future versions.

#### Interception
- **Command interception**: Command structure differs, affecting interception patterns
- **SaveChanges interception**: Flow differs from standard relational providers
- **Connection events**: Connection lifecycle event handling differs

#### Logging
- **Initialization logging**: Context initialization logging differs
- **Diagnostic events**: Some diagnostic events not emitted or formatted differently

#### Query Translation
- **Function translations**: Some EF.Functions methods not yet translated to YQL
- **Complex expressions**: Certain complex LINQ expressions not fully translated
- **Subquery optimization**: Some subquery patterns not optimally translated

#### Model Building
- **Owned entities**: Some owned entity patterns not fully supported
- **Table splitting**: Complex table splitting scenarios have gaps
- **Shared tables**: Certain shared table patterns not implemented

#### Connection Management
- **Connection strings**: Connection string parsing differs from ADO.NET standard
- **Connection pooling**: Session pooling behavior specific to YDB
- **Connection initialization**: Context-based connection initialization differs

## Test Categorization

Tests are marked with one of the following skip reasons:

- `"YDB server limitation - [specific reason]"` - Cannot be fixed without YDB server changes
- `"YDB provider implementation - [specific reason]"` - Could be fixed in the provider
- `"Provider implementation gap - [specific reason]"` - Known gap, fix planned or under consideration

## Contributing

When adding new test skips:
1. Clearly document whether it's a server or provider limitation
2. Provide specific reasons
3. Reference any related YDB issues if applicable
4. Use consistent skip message formatting

## References

- YDB Documentation: https://ydb.tech/docs
- YQL Reference: https://ydb.tech/docs/yql/reference/
- EFCore.Ydb Issues: https://github.com/ydb-platform/ydb-dotnet-sdk/issues
