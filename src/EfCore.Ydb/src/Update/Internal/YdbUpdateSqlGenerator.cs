using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore.Update;

namespace EfCore.Ydb.Update.Internal;

public class YdbUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies) : UpdateSqlGenerator(dependencies)
{
    public override ResultSetMapping AppendInsertReturningOperation(
        StringBuilder commandStringBuilder, IReadOnlyModificationCommand command, int commandPosition,
        out bool requiresTransaction
    )
    {
        var name = command.TableName;
        var schema = command.Schema;
        var operations = command.ColumnModifications;

        var writeOperations = operations.Where(o => o.IsWrite).ToList();
        var readOperations = operations.Where(o => o.IsRead).ToList();

        AppendInsertCommand(
            commandStringBuilder,
            name,
            schema,
            writeOperations,
            readOperations);

        requiresTransaction = false;

        return readOperations.Count > 0 ? ResultSetMapping.LastInResultSet : ResultSetMapping.NoResults;
    }

    protected override ResultSetMapping AppendUpdateReturningOperation(
        StringBuilder commandStringBuilder, IReadOnlyModificationCommand command, int commandPosition,
        out bool requiresTransaction
    )
    {
        var name = command.TableName;
        var schema = command.Schema;
        var operations = command.ColumnModifications;

        var writeOperations = operations.Where(o => o.IsWrite).ToList();
        var conditionOperations = operations.Where(o => o.IsCondition).ToList();
        var readOperations = operations.Where(o => o.IsRead).ToList();

        requiresTransaction = false;

        var anyReadOperations = readOperations.Count > 0;

        AppendUpdateCommand(
            commandStringBuilder,
            name,
            schema,
            writeOperations,
            readOperations,
            conditionOperations);

        return anyReadOperations
            ? ResultSetMapping.LastInResultSet
            : ResultSetMapping.LastInResultSet | ResultSetMapping.NoResults;
    }

    protected override ResultSetMapping AppendDeleteReturningOperation(
        StringBuilder commandStringBuilder, IReadOnlyModificationCommand command, int commandPosition,
        out bool requiresTransaction
    )
    {
        var name = command.TableName;
        var schema = command.Schema;
        var conditionOperations = command.ColumnModifications.Where(o => o.IsCondition).ToList();

        requiresTransaction = false;

        AppendDeleteCommand(
            commandStringBuilder,
            name,
            schema,
            [],
            conditionOperations
        );

        return ResultSetMapping.LastInResultSet | ResultSetMapping.NoResults;
    }

    protected override void AppendInsertCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        IReadOnlyList<IColumnModification> writeOperations,
        IReadOnlyList<IColumnModification> readOperations
    )
    {
        AppendInsertCommandHeader(commandStringBuilder, name, schema, writeOperations);
        AppendValuesHeader(commandStringBuilder, writeOperations);
        AppendValues(commandStringBuilder, name, schema, writeOperations);
        AppendReturningClause(commandStringBuilder, readOperations);
        commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);
    }

    protected override void AppendUpdateCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        IReadOnlyList<IColumnModification> writeOperations,
        IReadOnlyList<IColumnModification> readOperations,
        IReadOnlyList<IColumnModification> conditionOperations,
        bool appendReturningOneClause = false
    )
    {
        AppendUpdateCommandHeader(commandStringBuilder, name, schema, writeOperations);
        AppendWhereClause(commandStringBuilder, conditionOperations);
        AppendReturningClause(commandStringBuilder, readOperations);
        commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);
    }

    protected override void AppendDeleteCommand(
        StringBuilder commandStringBuilder,
        string name,
        string? schema,
        IReadOnlyList<IColumnModification> readOperations,
        IReadOnlyList<IColumnModification> conditionOperations,
        bool appendReturningOneClause = false
    )
    {
        AppendDeleteCommandHeader(commandStringBuilder, name, schema);
        AppendWhereClause(commandStringBuilder, conditionOperations);
        AppendReturningClause(
            commandStringBuilder,
            readOperations
        );
        commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);
    }

    public override ResultSetMapping AppendStoredProcedureCall(
        StringBuilder commandStringBuilder, IReadOnlyModificationCommand command, int commandPosition,
        out bool requiresTransaction
    ) => throw new InvalidOperationException("Stored procedure calls are not supported in YDB");

    protected override void AppendReturningClause(
        StringBuilder commandStringBuilder,
        IReadOnlyList<IColumnModification> operations,
        string? additionalValues = null
    )
    {
        if (operations.Count <= 0) return;

        commandStringBuilder
            .AppendLine()
            .Append("RETURNING ");
        foreach (var operation in operations)
        {
            SqlGenerationHelper.DelimitIdentifier(commandStringBuilder, operation.ColumnName);
        }
    }

    public override string GenerateNextSequenceValueOperation(string name, string? schema)
        => throw new InvalidOperationException("Iterating over serial is not supported in YDB");

    public override void AppendNextSequenceValueOperation(
        StringBuilder commandStringBuilder, string name, string? schema
    ) => throw new InvalidOperationException("Iterating over serial is not supported in YDB");

    public override string GenerateObtainNextSequenceValueOperation(string name, string? schema)
        => throw new InvalidOperationException("Iterating over serial is not supported in YDB");

    public override void AppendObtainNextSequenceValueOperation(
        StringBuilder commandStringBuilder, string name, string? schema
    ) => throw new InvalidOperationException("Iterating over serial is not supported in YDB");

    private static string? GetPrimaryKey(IReadOnlyModificationCommand command)
        => command.Table?.PrimaryKey?.Columns[0].Name;
}
