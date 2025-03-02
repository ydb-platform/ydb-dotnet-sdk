using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace EfCore.Ydb.Update.Internal;

public class YdbUpdateSqlGenerator : UpdateSqlGenerator
{
    public YdbUpdateSqlGenerator(
        UpdateSqlGeneratorDependencies dependencies
    ) : base(dependencies)
    {
    }


    public override ResultSetMapping AppendInsertOperation(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction)
    {
        var name = command.TableName;
        var schema = command.Schema;
        var operations = command.ColumnModifications;

        var writeOperations = operations.Where(o => o.IsWrite).ToList();
        var readOperations = operations.Where(o => o.IsRead).ToList();

        AppendInsertCommand(commandStringBuilder, name, schema, writeOperations, readOperations);

        requiresTransaction = false;

        return ResultSetMapping.NoResults;
    }

    protected override void AppendReturningClause(
        StringBuilder commandStringBuilder, IReadOnlyList<IColumnModification> operations, string? additionalValues = null
    )
    {
        // Ydb doesn't support RETURNING clause
    }
}
