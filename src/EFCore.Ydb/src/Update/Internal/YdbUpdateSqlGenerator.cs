using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Update;

namespace EntityFrameworkCore.Ydb.Update.Internal;

public class YdbUpdateSqlGenerator(UpdateSqlGeneratorDependencies dependencies) : UpdateSqlGenerator(dependencies)
{
    public override ResultSetMapping AppendInsertReturningOperation(
        StringBuilder commandStringBuilder,
        IReadOnlyModificationCommand command,
        int commandPosition,
        out bool requiresTransaction
    )
    {
        var name = command.TableName;
        var schema = command.Schema;
        var operations = command.ColumnModifications;
        var writeOperations = operations.Where(o => o.IsWrite && !IsStoreGeneratedAndIgnoredBeforeSave(o)).ToList();
        var readOperations = operations.Where(o => o.IsRead||IsStoreGeneratedAndIgnoredBeforeSave(o)).ToList();

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
        var writeOperations = operations.Where(o => o.IsWrite && !IsStoreGeneratedAndIgnoredBeforeSave(o)).ToList();
        var conditionOperations = operations.Where(o => o.IsCondition).ToList();
        var readOperations = operations.Where(o => o.IsRead||IsStoreGeneratedAndIgnoredBeforeSave(o)).ToList();

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
            : ResultSetMapping.NoResults;
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

        AppendDeleteCommand(commandStringBuilder, name, schema, [], conditionOperations);

        return ResultSetMapping.NoResults;
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
        var effectiveWrites = writeOperations;
        if (effectiveWrites.Count == 0)
        {
            var noOpColumn = GetNoOpSetColumn(conditionOperations, readOperations);
            AppendUpdateCommandHeader(commandStringBuilder, name, schema, noOpColumn is null ? effectiveWrites : new[] { noOpColumn });
        }
        else
        {
            AppendUpdateCommandHeader(commandStringBuilder, name, schema, effectiveWrites);
        }
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
        AppendReturningClause(commandStringBuilder, readOperations);
        commandStringBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);
    }

    public override ResultSetMapping AppendStoredProcedureCall(
        StringBuilder commandStringBuilder, IReadOnlyModificationCommand command, int commandPosition,
        out bool requiresTransaction
    ) => throw new NotSupportedException("Stored procedure calls are not supported in YDB");

    protected override void AppendReturningClause(
        StringBuilder commandStringBuilder,
        IReadOnlyList<IColumnModification> operations,
        string? additionalValues = null
    )
    {
        if (operations.Count <= 0 && string.IsNullOrEmpty(additionalValues)) return;

        commandStringBuilder.AppendLine().Append("RETURNING ");
        var columns = operations.Select(o => SqlGenerationHelper.DelimitIdentifier(o.ColumnName)).ToList();
        if (!string.IsNullOrEmpty(additionalValues)) columns.Add(additionalValues!);
        commandStringBuilder.AppendJoin(',', columns);
    }

    public override string GenerateNextSequenceValueOperation(string name, string? schema)
        => throw new NotSupportedException("Iterating over serial is not supported in YDB");

    public override void AppendNextSequenceValueOperation(
        StringBuilder commandStringBuilder, string name, string? schema
    ) => throw new NotSupportedException("Iterating over serial is not supported in YDB");

    public override string GenerateObtainNextSequenceValueOperation(string name, string? schema)
        => throw new NotSupportedException("Iterating over serial is not supported in YDB");

    public override void AppendObtainNextSequenceValueOperation(
        StringBuilder commandStringBuilder, string name, string? schema
    ) => throw new NotSupportedException("Iterating over serial is not supported in YDB");

    private static bool IsStoreGeneratedAndIgnoredBeforeSave(IColumnModification op)
    {
        var p = op.Property;
        if (p == null) return false;
        if (p.ValueGenerated != ValueGenerated.OnAdd && p.ValueGenerated != ValueGenerated.OnAddOrUpdate) return false;
        return p.GetBeforeSaveBehavior() == PropertySaveBehavior.Ignore;
    }

    private static IColumnModification? GetNoOpSetColumn(
        IReadOnlyList<IColumnModification> conditionOperations,
        IReadOnlyList<IColumnModification> readOperations
    )
    {
        var candidate = conditionOperations.FirstOrDefault() ?? readOperations.FirstOrDefault();
        return candidate;
    }
}
