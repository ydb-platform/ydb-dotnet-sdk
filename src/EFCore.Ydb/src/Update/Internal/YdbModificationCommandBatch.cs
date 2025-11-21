using Microsoft.EntityFrameworkCore.Update;

namespace EntityFrameworkCore.Ydb.Update.Internal;

/// <summary>
/// YDB modification command batch.
/// 
/// NOTE: Currently uses a conservative batch size of 500 to avoid YQL text size limits.
/// YDB server has a limit of 131KB for query text. With individual SQL statements for each operation,
/// batches larger than 100-200 commands can exceed this limit.
/// 
/// TODO: Implement LIST&lt;STRUCT&gt; based batching to enable much larger batch sizes (1000+).
/// This would use YDB's AS_TABLE pattern for more compact query representation:
/// - INSERT INTO table SELECT * FROM AS_TABLE($values)
/// - UPDATE table ON SELECT * FROM AS_TABLE($values)
/// - DELETE FROM table ON SELECT * FROM AS_TABLE($values)
/// 
/// This requires overriding SQL generation at the batch level to group commands by operation type
/// and table, then generating struct-based queries instead of individual statements.
/// </summary>
public class YdbModificationCommandBatch(
    ModificationCommandBatchFactoryDependencies dependencies
) : AffectedCountModificationCommandBatch(dependencies, 500 /* Conservative limit - see TODO above */);
