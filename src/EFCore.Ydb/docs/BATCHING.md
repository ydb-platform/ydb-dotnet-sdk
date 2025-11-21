# YDB Entity Framework Core Batching

## Overview

Entity Framework Core batches multiple modification operations (INSERT, UPDATE, DELETE) into a single database round-trip for better performance. However, YDB has constraints that affect batching:

- **YQL Text Size Limit**: 131KB maximum query text size
- **Traditional Batching**: Generates individual SQL statements like:
  ```sql
  UPDATE `Table` SET `Col` = @p0 WHERE `Id` = @p1;
  INSERT INTO `Table` (`Col1`, `Col2`) VALUES (@p2, @p3);
  INSERT INTO `Table` (`Col1`, `Col2`) VALUES (@p4, @p5);
  ```

With traditional batching, large batches (>100-200 operations) can exceed the 131KB limit.

## Current Implementation

**Batch Size**: 500 operations (increased from 100)

This provides a balance between:
- Avoiding the 131KB YQL text limit with traditional SQL generation
- Enabling reasonable batch sizes for common use cases

## LIST<STRUCT> Batching (Future Enhancement)

YDB supports a more compact batching pattern using `LIST<STRUCT<...>>`:

### Example: Bulk Insert
```csharp
var structs = new List<YdbStruct>();
for (int i = 0; i < 1000; i++)
{
    structs.Add(new YdbStruct
    {
        { "Id", i },
        { "Name", $"Item {i}" },
        { "Value", i * 10 }
    });
}

await using var cmd = new YdbCommand(
    "INSERT INTO `MyTable` SELECT * FROM AS_TABLE($values)",
    connection);
cmd.Parameters.Add(new YdbParameter("values", structs));
await cmd.ExecuteNonQueryAsync();
```

### Example: Bulk Update
```csharp
var structs = new List<YdbStruct>();
for (int i = 0; i < 1000; i++)
{
    structs.Add(new YdbStruct
    {
        { "Id", i },
        { "Name", $"Updated {i}" },
        { "Value", i * 20 }
    });
}

await using var cmd = new YdbCommand(
    "UPDATE `MyTable` ON SELECT * FROM AS_TABLE($values)",
    connection);
cmd.Parameters.Add(new YdbParameter("values", structs));
await cmd.ExecuteNonQueryAsync();
```

### Example: Bulk Delete
```csharp
var structs = new List<YdbStruct>();
for (int i = 0; i < 1000; i++)
{
    structs.Add(new YdbStruct
    {
        { "Id", i }
    });
}

await using var cmd = new YdbCommand(
    "DELETE FROM `MyTable` ON SELECT * FROM AS_TABLE($values)",
    connection);
cmd.Parameters.Add(new YdbParameter("values", structs));
await cmd.ExecuteNonQueryAsync();
```

### Benefits

- **Compact Representation**: Query text stays small regardless of batch size
- **Better Performance**: Can batch 1000+ operations without hitting text limits
- **Reduced Network Overhead**: Single parameter instead of hundreds of individual parameters

### Integration with EF Core (Roadmap)

To integrate LIST<STRUCT> batching with EF Core:

1. **Override Batch SQL Generation**:
   - Group `ModificationCommand` objects by operation type and table
   - Generate struct-based SQL instead of individual statements
   - Convert column modifications to `YdbStruct` objects

2. **Handle Parameters**:
   - Create `List<YdbStruct>` from command collections
   - Pass as single `YdbParameter` instead of multiple individual parameters

3. **Maintain Compatibility**:
   - Fall back to traditional batching for mixed operations
   - Handle commands with RETURNING clauses properly
   - Preserve transaction semantics

### Implementation Challenges

- **EF Core Architecture**: The framework generates SQL statement-by-statement
- **Complex Override**: Requires deep integration with EF Core's update pipeline
- **Result Mapping**: RETURNING clauses need proper handling for generated values
- **Mixed Batches**: Different operations/tables need appropriate grouping strategy

## Workaround for Large Batches

If you need to batch more than 500 operations today, you can:

1. **Use SaveChanges() More Frequently**: Call `SaveChanges()` every 500 entities
2. **Use Raw YDB SDK**: Bypass EF Core for bulk operations using the examples above
3. **Split Operations**: Group by entity type to get better batching per type

```csharp
// Example: Saving 2000 entities
for (int i = 0; i < 2000; i += 500)
{
    var batch = entities.Skip(i).Take(500);
    context.AddRange(batch);
    await context.SaveChangesAsync();
}
```

## Performance Benchmarks

| Batch Size | Traditional SQL Size | LIST<STRUCT> Size | Status |
|------------|----------------------|-------------------|--------|
| 100 | ~50KB | ~10KB | ✅ Supported |
| 500 | ~250KB | ~50KB | ✅ Supported (current limit) |
| 1000 | ~500KB | ~100KB | ❌ Exceeds limit / ✅ With LIST<STRUCT> |
| 5000 | ~2.5MB | ~500KB | ❌ / ✅ With LIST<STRUCT> |

*Sizes are approximate and vary based on column count and data types*

## Contributing

We welcome contributions to implement full LIST<STRUCT> batching support! See the implementation challenges section above for guidance.

## Related

- [YDB SDK List<YdbStruct> Tests](../../../Ydb.Sdk/test/Ydb.Sdk.Ado.Tests/YdbStructTest.cs)
- [YDB Documentation: AS_TABLE](https://ydb.tech/docs/en/yql/reference/syntax/insert_into#as-table)
