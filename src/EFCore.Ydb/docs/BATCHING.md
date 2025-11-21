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

**Batch Size**: 1000 operations (increased from 100)

The implementation uses LIST<STRUCT> batching when possible:
- When all commands in a batch are for the same table, operation type, and schema
- Commands are grouped and executed using YDB's `AS_TABLE($values)` pattern
- Falls back to traditional statement-by-statement execution for mixed batches

This provides:
- 10x increase in batch capacity (100 → 1000)
- Significantly reduced query text size for homogeneous batches
- Maintained compatibility with mixed operation scenarios

## LIST<STRUCT> Batching

YDB supports a compact batching pattern using `LIST<STRUCT<...>>`. The EF Core YDB provider now automatically uses this pattern in `SaveChanges()` when batching homogeneous operations:

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

### How It Works in EF Core

The YDB provider automatically uses struct-based batching in `SaveChanges()` when:

1. **Detects homogeneous batches**: All operations are the same type (INSERT/UPDATE/DELETE) and target the same table
2. **Generates compact SQL**: Creates a single query using `AS_TABLE($batch_values)` pattern
3. **Packages data efficiently**: Converts all entity changes into a `List<YdbStruct>` parameter
4. **Falls back gracefully**: Uses traditional batching for mixed operations or cross-table changes

Example of what happens internally when you save 500 new entities:

```csharp
// Your code
context.Products.AddRange(products);  // 500 products
await context.SaveChangesAsync();

// What the provider generates (simplified):
// INSERT INTO `Products` (Id, Name, Price)
// SELECT Id, Name, Price FROM AS_TABLE($batch_values)
// 
// Where $batch_values contains all 500 products as a compact List<YdbStruct>
```

### Implementation Details

The struct-based batching is implemented in `YdbModificationCommandBatch`:
- Checks if batching is possible via `CanUseStructBatching()`
- Overrides `Execute/ExecuteAsync` to generate struct-based SQL
- Converts entity changes to `YdbStruct` objects automatically
- Handles nullable columns and type mappings

## Working with Large Batches

With struct-based batching, you can now safely batch up to 1000 operations:

```csharp
// Efficient: SaveChanges will use struct batching automatically
for (int i = 0; i < 5000; i++)
{
    context.Products.Add(new Product { Name = $"Product {i}", Price = i * 10 });
}
await context.SaveChangesAsync();  // Processes in batches of 1000 using struct-based SQL
```

For batches exceeding 1000 operations, chunk your saves:

```csharp
// Example: Saving 5000 entities in chunks
for (int i = 0; i < 5000; i += 1000)
{
    var batch = entities.Skip(i).Take(1000);
    context.AddRange(batch);
    await context.SaveChangesAsync();  // Each uses struct batching
}
```

For mixed operations (different entity types or operation types), EF Core will still batch them but may use traditional SQL if they can't be grouped homogeneously.

## Performance Benchmarks

| Batch Size | Traditional SQL Size | LIST<STRUCT> Size | Status |
|------------|----------------------|-------------------|--------|
| 100 | ~50KB | ~10KB | ✅ Supported (both modes) |
| 500 | ~250KB | ~50KB | ✅ Supported (both modes) |
| 1000 | ~500KB | ~100KB | ❌ Traditional / ✅ Struct-based (default) |
| 5000 | ~2.5MB | ~500KB | ❌ / ✅ Struct-based with chunking |

*Sizes are approximate and vary based on column count and data types*

**Note**: Struct-based batching is automatically used for homogeneous batches (same operation + table). Mixed batches fall back to traditional mode with appropriate batch size limits.

## Related

- [YDB SDK List<YdbStruct> Tests](../../../Ydb.Sdk/test/Ydb.Sdk.Ado.Tests/YdbStructTest.cs)
- [YDB Documentation: AS_TABLE](https://ydb.tech/docs/en/yql/reference/syntax/insert_into#as-table)
