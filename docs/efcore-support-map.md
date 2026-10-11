# EntityFrameworkCore.Ydb support map

Source baseline: `1add277` (2026-10-11). This is an implementation and test
inventory, not a claim that every EF Core contract is supported. Runtime results
for the first functional increment are recorded separately in its PR.

Paths below are relative to `src/EFCore.Ydb`; test names refer to
`test/EntityFrameworkCore.Ydb.FunctionalTests`.

## Versions and execution path

`src/EntityFrameworkCore.Ydb.csproj` targets net8.0 / EF Core 9.0.0 and
net10.0 / EF Core 10.0.0. The functional project targets net9.0 / specification
tests 9.0.1 and net10.0 / specification tests 10.0.0.
The `efcore-tests` CI job currently runs only net9.0.

The provider references published `Ydb.Sdk` 0.33.3. Functional tests additionally
reference the local SDK project. Passing those tests alone does not establish
behavior with the provider's published SDK dependency; a consumer without that
project reference is also needed.

1. `Extensions/YdbContextOptionsBuilderExtensions.cs:12` configures `UseYdb`;
   `Extensions/YdbServiceCollectionExtensions.cs:25` registers relational services.
2. EF's query compilation pipeline uses the provider's queryable and SQL
   translating visitor factories, method/member/aggregate translators, expression
   factory and translation/type-mapping postprocessors.
3. `Query/Internal/YdbParameterBasedSqlProcessor.cs:13` dispatches runtime
   nullability processing to `YdbSqlNullabilityProcessor` using the EF 9 parameter
   dictionary or EF 10 parameter cache decorator. The SQL generator emits YQL.
4. Relational type mappings configure ADO parameters.
   `Storage/Internal/Mapping/YdbTypeMapping.cs:29` sets `YdbParameter.YdbDbType`;
   `Storage/Internal/YdbRelationalConnection.cs:29` creates `YdbConnection`.
5. EF's relational commands execute through the SDK's `Ado/YdbCommand.cs`.
   EF's materializer consumes `Ado/YdbDataReader.cs` with the provider mappings.
   These SDK paths are under `src/Ydb.Sdk/src`, not this provider.

## Implementation and coverage

| Area | Implementation | Coverage | Boundaries / evidence |
| --- | --- | --- | --- |
| Services/options | `Extensions/YdbServiceCollectionExtensions.cs:25`, `Infrastructure/Internal/YdbOptionsExtension.cs` | `YdbServiceCollectionExtensionsTest`, `DesignTimeYdbTest` | Provider factories replace selected relational services; other services are inherited. Source evidence. |
| Conventions/model validation | `Metadata/Conventions/YdbConventionSetBuilder.cs`, `YdbStringAttributeConvention.cs`, `Infrastructure/Internal/YdbModelValidator.cs:5` | `ModelBuilding`, `ModelBuilding101YdbTest`, `KeysWithConvertersYdbTest` | `[YdbString]` convention exists. Model validator has no YDB-specific checks beyond its relational base. Source evidence, not proof every accepted model is executable. |
| Query translation | `Query/Internal/YdbQueryableMethodTranslatingExpressionVisitor.cs`, `YdbSqlTranslatingExpressionVisitor.cs`, `Translators/` | Inherited `Query/` suites; `SkipTakeYdbTest` | Specialized temporal/math/bytes/ILIKE translators; many navigation/join/correlated shapes remain skipped or no-op. Query inventory below. |
| Nullability | `Query/Internal/YdbSqlNullabilityProcessor.cs:20`, parameter-based processor/factory | `Query/NullKeysYdbTest`, literal ILIKE cases in `NorthwindCompiledQueryYdbTest`, new `ILikeParameterYdbTest` | Custom ILIKE expression requires its own visitor. At the baseline, `IsNull` considers every parameter null. Confirmed wrong-result defect; first increment checks actual parameter values on EF 9/10. |
| SQL/YQL generation | `Query/Internal/YdbQuerySqlGenerator.cs` | Query SQL baselines, `SkipTakeYdbTest`, bulk tests | Backticks, Utf8 literals, required offset-only LIMIT, string `||`, JSON_VALUE, DELETE ON / UPDATE ON. PostgreSQL SQL is not the contract. |
| Parameters/types | `Storage/Internal/YdbTypeMappingSource.cs:16`, `Storage/Internal/Mapping/` | `DecimalParameterizedYdbTest`, `JsonTypeMappingTests`, `SqlQueryCollectionParameterTests`, `InsertWithDefaultsTests` | Decimal precision/scale, typed lists, JSON, Guid and extended temporal mappings exist. CLR mapping does not imply every YDB store-type alias/precision is supported. Disabled conversion suite's `(22,9)` comment is not a current server limit. |
| Materialization/tracking | Relational EF materialization + SDK reader, provider mappings | `MaterializationInterceptionYdbTest`, `WithConstructorsYdbTest`, `ComplexTypesTrackingYdbTest`, `PropertyValuesYdbTest` | Some inherited cases are no-op overrides; suite totals can overstate exercised behavior. |
| SaveChanges/batching | `Update/Internal/YdbModificationCommandBatch.cs:15`, `YdbUpdateSqlGenerator.cs` | `YdbModificationCommandBatchTest`, `SerialColumnsCrudTests`, defaults tests | Compatible commands become typed `List<Struct>` / AS_TABLE batches (15,000 limit). Different state/table/schema/columns split batches. Non-key concurrency conditions, key modification and non-parameter shapes use the base path. Returned generated values and affected-count/concurrency behavior need separate assertions; not inferred from SQL. |
| Bulk DML | Version-specific ExecuteDelete visitors, query visitor and generator | `BulkUpdates/CorrelatedExecuteDeleteYdbTest`, `CorrelatedExecuteUpdateYdbTest`, Northwind/complex-type suites | Correlated selections use primary-key projections for DELETE ON / UPDATE ON. Existing support is not removed. Many inherited shapes still skipped; generic skip text does not identify the failing layer. |
| Transactions | `Storage/Internal/YdbRelationalTransaction.cs:21`, factory + SDK transaction | `TransactionInterceptionYdbTest` | Savepoints explicitly unsupported. Supported isolation levels depend on SDK/server; unsupported-level tests are skipped. No PostgreSQL transaction assumptions. |
| Execution strategy | `Storage/Internal/YdbExecutionStrategy.cs:30`, factory/options + SDK retry policy | `YdbExecutionStrategyMetricTests`, `Tracing/YdbExecutionStrategyTracingTests` | Transient statuses retried; uncertain transport outcomes/Undetermined require configured idempotence. Telemetry wraps attempts. Do not infer safe replay of non-idempotent writes. |
| Migrations/history | `Migrations/YdbMigrationsSqlGenerator.cs:12`, `Migrations/Internal/YdbHistoryRepository.cs:18` | `Migrations/YdbMigrationsSqlGeneratorTest`, `YdbMigrationsTest`, `YdbMigrationsInfrastructureTest` | SERIAL mapping and defaults already exist. Computed columns explicitly rejected; DDL transaction suppression and history-row locking are provider-specific. Idempotent script hooks throw. Infrastructure suite has 21 skip markers; broad migration support claims need qualification. |
| Design-time/scaffolding | `Design/Internal/YdbDesignTimeServices.cs:10`, `Scaffolding/Internal/YdbDatabaseModelFactory.cs:15` | `DesignTimeYdbTest`, model-building tests | Registers relational design services without runtime retry strategy; reads tables/columns/indexes/keys using SDK schema APIs. Does not imply foreign-key or sequence scaffolding support. |

## Test-gap inventory

At the baseline there are **585 active source-level Skip attributes** in 24
files (including 12 in an internal, undiscovered update class). These are not
585 executed test rows: theories, inherited/nested classes and discovery expand
or omit cases. Eight additional Skip attributes are commented out in
`BulkUpdates/TPTFiltersInheritanceBulkUpdatesYdbTest.cs`.

| File / group | Active Skip attributes | Classification and next proof |
| --- | ---: | --- |
| `BulkUpdates/ComplexTypeBulkUpdatesYdbTest` | 1 | Claimed server syntax limit (`UPDATE FROM`). Reproduce the inherited shape and compare generated UPDATE ON before assigning fault. |
| `BulkUpdates/NorthwindBulkUpdatesYdbTest` | 21 | Unknown: generic bulk-shape restriction. Run each inherited scenario; distinguish translation from server rejection and affected rows. |
| `BulkUpdates/TPCFiltersInheritanceBulkUpdatesYdbTest`, `TPCInheritanceBulkUpdatesYdbTest`, `TPHFiltersInheritanceBulkUpdatesYdbTest`, `TPHInheritanceBulkUpdatesYdbTest` | 5 + 4 + 5 + 5 | Claimed server issue [YDB #15177](https://github.com/ydb-platform/ydb/issues/15177). Issue reference alone is not current server evidence. |
| `CompositeKeyEndToEndTest` | 1 | Unknown / possible infrastructure: "Cannot access table". Reproduce setup and generated composite-key readback. |
| `FieldMappingYdbTest`, `Query/AdHocAdvancedMappingsQueryYdbTest` | 1 + 1 | Provider/projection candidate: ORDER BY item missing from SELECT. Capture actual SQL and server error. |
| `FindYdbTest` | 4 | Unknown: "Requires fixes". Verify existing/missing keys through set/context/non-generic variants. |
| `LazyLoadProxyYdbTest` | 3 | Unknown: precision / InvalidOperationException. Separate mapping from proxy/query behavior. |
| `Migrations/YdbMigrationsInfrastructureTest` | 21 | Unknown: TODO only. Reproduce migration setup, history, locking and cancellation independently. |
| `ModelBuilding/YdbModelBuilderGenericTest` | 5 | Unknown: "Not ready yet". Test model validation/design-time contract without assuming a database limitation. |
| `Query/ComplexNavigationsCollectionsQueryYdbTest`, `ComplexNavigationsCollectionsSharedTypeQueryYdbTest` | 30 + 34 | Claimed correlated-subquery limitation, not independently confirmed for all shapes. |
| `Query/ComplexNavigationsQueryYdbTest` | 83 | Claimed non-equality INNER JOIN restriction; source comment includes an example. Validate server-version syntax and whether predicates can move safely. |
| `Query/ComplexNavigationsSharedTypeQueryYdbTest` | 79 | Mixed correlated/projection claims; needs individual reproduction. |
| `Query/CompositeKeysQueryYdbTest` | 1 | Provider translation candidate: multi-level collection projection. Reduce actual expression. |
| `Query/TPTGearsOfWarQueryYdbTest` | 261 | TODO-only / correlated-query claims. No universal server conclusion. |
| `TPTTableSplittingYdbTest` | 2 | Unknown: empty sequence or ORDER BY projection. Capture actual failure. |
| `TransactionInterceptionYdbTest` | 2 | Unsupported isolation level claim; check exact SDK/server contract. Savepoints are explicitly unsupported in production. |
| `TwoDatabasesYdbTest` | 3 | Infrastructure: fixture exposes one database. Not proof multi-database connections are unsupported. |
| `Update/UpdatesYdbTest` | 12 | Internal class is not discovered; source markers are not runtime coverage. |
| `WithConstructorsYdbTest` | 1 | Claimed keyless table restriction. Distinguish keyless EF query model from creating a keyless YDB table. |

Undiscovered internal suites include `ConvertToProviderTypesYdbTest`,
`Query/GearsOfWarQueryYdbTest`, `Query/TPCGearsOfWarQueryYdbTest`,
`Update/UpdatesYdbTest` and the two TPT bulk suites. The decimal TODO and old
success-rate comments need revalidation, not acceptance as facts.

No-op overrides also occur in `NorthwindCompiledQueryYdbTest` (nine),
`NorthwindGroupByQueryYdbTest`, `NorthwindAggregateOperatorsQueryYdbTest`,
`AdHocAdvancedMappingsQueryYdbTest`, `AdHocNavigationsQueryYdbTest`,
`AdHocQueryFiltersQueryYdbTest`, `EntitySplittingQueryYdbTest`,
`PropertyValuesYdbTest`, migration and transaction tests. Returning
`Task.CompletedTask` or an empty body is not a passing specification scenario.
No skips, no-ops or suppressions are added by the first increment.

## First finite stage

The first increment fixes parameterized ILIKE translation. On YDB
`26.1.1.22.1b59297`, both EF Core versions reproduce a non-null `"al%"` search
returning no rows instead of the seeded `"aLpHa"` row. A standalone consumer
using published SDK 0.33.3 reproduces the same defect; functional tests use the
local SDK. Direct YQL with a declared Utf8 pattern parameter returns true.

The fix checks actual values in the existing nullability processor and adds a
small `IEvaluatableExpressionFilter` specialization. With a captured match and
literal pattern, EF otherwise invokes the server-only extension method on the
client before its translator can run. The filter excludes only methods declared
on `YdbFunctionExtension`, delegating every other expression to the relational
base. SQL generation, type mappings, retries and migration behavior are unchanged.
`Query/ILikeParameterYdbTest.cs` uses four rows rather than the large Northwind
fixture, asserting non-null/null parameter transitions on reused queries,
nullable stored values, negation, materialized boolean projection and a
parameterized match operand. The standalone consumer additionally checks
compiled queries, literal escaping, LIKE, equality and unequal Skip/Take.

Architectural analog:
[Npgsql 9.0.0](https://github.com/npgsql/efcore.pg/blob/v9.0.0/src/EFCore.PG/Query/Internal/NpgsqlSqlNullabilityProcessor.cs)
uses `ParameterValues[name] is null`;
[Npgsql 10.0.0](https://github.com/npgsql/efcore.pg/blob/v10.0.0/src/EFCore.PG/Query/Internal/NpgsqlSqlNullabilityProcessor.cs)
uses `ParametersDecorator.IsNull(name)`.
Both specialize the custom expression in the parameter-based
`SqlNullabilityProcessor`, following upstream EF's `VisitLike`.
Upstream references:
[EF Core 9](https://github.com/dotnet/efcore/blob/v9.0.0/src/EFCore.Relational/Query/SqlNullabilityProcessor.cs)
and [EF Core 10](https://github.com/dotnet/efcore/blob/v10.0.0/src/EFCore.Relational/Query/SqlNullabilityProcessor.cs).
The processor has the current parameter values and participates in EF's
nullability-dependent command cache; a translator or SQL generator does not
have that runtime context. YDB keeps its own ILIKE/Utf8/ESCAPE SQL and server
semantics, checked against
[YQL expressions](https://ydb.tech/docs/en/yql/reference/syntax/expressions)
and the pinned local server rather than inferred from PostgreSQL.

The second hook follows
[Npgsql 9's evaluatable-expression filter](https://github.com/npgsql/efcore.pg/blob/v9.0.0/src/EFCore.PG/Query/Internal/NpgsqlEvaluatableExpressionFilter.cs)
and [Npgsql 10's filter](https://github.com/npgsql/efcore.pg/blob/v10.0.0/src/EFCore.PG/Query/Internal/NpgsqlEvaluatableExpressionFilter.cs).
Both providers register `IEvaluatableExpressionFilter` in their service
collection and prevent local evaluation of provider-specific DbFunctions.
YDB needs only its own extension type, not Npgsql's PostgreSQL-specific
tuple, full-text, network or UUID rules.

The server also rejects a nullable column used as the pattern, independently of
the parameter bug:

```yql
SELECT Id FROM ILikeEntities WHERE 'ALPHA'u ILIKE Name;
```

With `Name Utf8` nullable, YDB 26.1.1.22 reports
`Re2.PatternFromLike: String!=Optional<Utf8>`. Casting `Name` to `String`
still fails with `String!=Optional<String>`. This PR does not add support for
nullable column patterns. Its match-parameter coverage uses a literal pattern.
The next step for column patterns is to check YDB's pattern/UDF nullability
contract and a minimal server expression before choosing another provider
extension; PostgreSQL's acceptance is not evidence of YDB support.

After this PR, prioritize reproducing skipped basic Find cases, then missing
ORDER BY projections, then migration-lock failures. Each is a separate finite
investigation/fix/PR, not an authorization for adjacent refactoring.
