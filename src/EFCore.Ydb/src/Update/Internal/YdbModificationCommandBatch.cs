using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using EntityFrameworkCore.Ydb.Storage.Internal.Mapping;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Update.Internal;

public class YdbModificationCommandBatch(ModificationCommandBatchFactoryDependencies dependencies)
    : AffectedCountModificationCommandBatch(dependencies, StructBatchSize)
{
    private const int StructBatchSize = 15_000;

    private readonly List<IReadOnlyModificationCommand> _currentBatchCommands = [];
    private readonly List<string> _currentBatchColumns = [];

    private EntityState _currentBatchState = EntityState.Detached;
    private string _currentBatchTableName = null!;
    private string? _currentBatchSchema;
#pragma warning disable CA1859
    private IBatchHelper _batchHelper = null!; // TODO UPDATE / DELETE BatchHelper
#pragma warning restore CA1859
    private int _batchNumber;

    private ISqlGenerationHelper SqlGenerationHelper => Dependencies.SqlGenerationHelper;

    protected override void AddCommand(IReadOnlyModificationCommand modificationCommand)
    {
        if (_currentBatchColumns.Count == 0)
        {
            StartNewBatch(modificationCommand);
            return;
        }

        if (CanJoinBatch(modificationCommand))
        {
            _currentBatchCommands.Add(modificationCommand);
        }
        else
        {
            FlushBatch();
            StartNewBatch(modificationCommand);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool CanJoinBatch(IReadOnlyModificationCommand modificationCommand) =>
        _currentBatchState == modificationCommand.EntityState
        && string.Equals(_currentBatchTableName, modificationCommand.TableName)
        && string.Equals(_currentBatchSchema, modificationCommand.Schema)
        && _batchHelper.StructColumns(modificationCommand)
            .Select((t, i) => t.ColumnName)
            .SequenceEqual(_currentBatchColumns);

    public override void Complete(bool moreBatchesExpected)
    {
        FlushBatch();
        base.Complete(moreBatchesExpected);
    }

    private void StartNewBatch(IReadOnlyModificationCommand firstCommand)
    {
        if (firstCommand.EntityState != EntityState.Added)
        {
            base.AddCommand(firstCommand);
            _currentBatchCommands.Clear();
            _currentBatchState = firstCommand.EntityState;
            _currentBatchTableName = null!;
            _currentBatchSchema = null;
            _currentBatchColumns.Clear();
            return;
        }

        _batchHelper = InsertBatchHelper.Instance;
        _currentBatchCommands.Clear();
        _currentBatchCommands.Add(firstCommand);
        _currentBatchState = firstCommand.EntityState;
        _currentBatchTableName = firstCommand.TableName;
        _currentBatchSchema = firstCommand.Schema;
        _currentBatchColumns.Clear();
        foreach (var columnModification in _batchHelper.StructColumns(firstCommand))
        {
            _currentBatchColumns.Add(columnModification.ColumnName);
        }
    }

    private void FlushBatch()
    {
        switch (_currentBatchCommands.Count)
        {
            case 0:
                return;
            case 1:
                base.AddCommand(_currentBatchCommands[0]);
                return;
        }

        var batchParamName = $"$batch_value_{_batchNumber++}";
        var firstBatchCommand = _currentBatchCommands[0];
        _batchHelper.AppendSql(SqlBuilder,
            SqlGenerationHelper.DelimitIdentifier(_currentBatchTableName, _currentBatchSchema), batchParamName);
        var readColumns = firstBatchCommand.ColumnModifications.Where(c => c.IsRead).ToList();
        var hasReadColumns = readColumns.Count > 0;

        if (hasReadColumns)
        {
            SqlBuilder.AppendLine();
            SqlBuilder.Append("RETURNING ");
            SqlBuilder.AppendJoin(", ", readColumns.Select(c => SqlGenerationHelper.DelimitIdentifier(c.ColumnName)));
        }

        SqlBuilder.AppendLine(SqlGenerationHelper.StatementTerminator);

        var ydbStructValues = new List<YdbStruct>();
        for (var i = 0; i < _currentBatchCommands.Count - 1; i++)
        {
            ydbStructValues.Add(YdbStruct(i));
            ResultSetMappings.Add(hasReadColumns ? ResultSetMapping.NotLastInResultSet : ResultSetMapping.NoResults);
        }

        ydbStructValues.Add(YdbStruct(_currentBatchCommands.Count - 1));
        ResultSetMappings.Add(hasReadColumns ? ResultSetMapping.LastInResultSet : ResultSetMapping.NoResults);
        RelationalCommandBuilder.AddRawParameter(batchParamName, new YdbParameter(batchParamName, ydbStructValues));
    }

    private YdbStruct YdbStruct(int i)
    {
        var ydbStruct = new YdbStruct();
        foreach (var columnModification in _batchHelper.StructColumns(_currentBatchCommands[i]))
        {
            AddColumnToStruct(ydbStruct, columnModification);
        }

        return ydbStruct;
    }

    private static void AddColumnToStruct(YdbStruct ydbStruct, IColumnModification columnModification)
    {
        var mapping = columnModification.TypeMapping ?? throw new InvalidOperationException(
            $"TypeMapping is null for column '{columnModification.ColumnName}'.");

        var ydbDbType = mapping is IYdbTypeMapping ydbTypeMapping
            ? ydbTypeMapping.YdbDbType
            : mapping.DbType?.ToYdbDbType() ?? throw new InvalidOperationException(
                $"Could not determine YDB type for column '{columnModification.ColumnName}' (no IYdbTypeMapping and DbType is null).");

        ydbStruct.Add(
            columnModification.ColumnName,
            mapping.Converter != null
                ? mapping.Converter.ConvertToProvider(columnModification.Value)
                : columnModification.Value,
            ydbDbType,
            (byte)(mapping.Precision ?? 0),
            (byte)(mapping.Scale ?? 0)
        );
    }

    private interface IBatchHelper
    {
        IEnumerable<IColumnModification> StructColumns(IReadOnlyModificationCommand modificationCommand);

        void AppendSql(StringBuilder sql, string tableName, string paramName);
    }

    private class InsertBatchHelper : IBatchHelper
    {
        public static readonly InsertBatchHelper Instance = new();

        public IEnumerable<IColumnModification> StructColumns(IReadOnlyModificationCommand modificationCommand) =>
            modificationCommand.ColumnModifications.Where(c => c.IsWrite);

        public void AppendSql(StringBuilder sql, string tableName, string paramName)
        {
            // Generate INSERT INTO {TableName} SELECT * FROM AS_TABLE($param)
            sql.Append("INSERT INTO ");
            sql.Append(tableName);
            sql.Append(" SELECT * FROM AS_TABLE(");
            sql.Append(paramName);
            sql.Append(')');
        }
    }
}
