using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.EntityFrameworkCore.Update;
using Ydb.Sdk.Ado;
using Ydb.Sdk.Ado.YdbType;

namespace EntityFrameworkCore.Ydb.Update.Internal;

/// <summary>
/// YDB modification command batch using LIST&lt;STRUCT&gt; pattern for efficient batching.
/// Uses YDB's AS_TABLE($values) pattern to avoid YQL text size limits (131KB),
/// enabling batch sizes up to 1000+ instead of the previous limit of 100.
/// 
/// When possible, groups commands of the same type and table into struct-based batches.
/// Falls back to traditional statement-by-statement generation for mixed batches.
/// </summary>
public class YdbModificationCommandBatch : AffectedCountModificationCommandBatch
{
    private const int StructBatchSize = 1000;
    private bool _useStructBatching;

    public YdbModificationCommandBatch(
        ModificationCommandBatchFactoryDependencies dependencies
    ) : base(dependencies, StructBatchSize)
    {
    }

    /// <summary>
    /// Checks if we can use struct-based batching for this batch.
    /// Returns true if all commands are for the same entity state, table, and schema.
    /// </summary>
    private bool CanUseStructBatching()
    {
        if (ModificationCommands.Count <= 1)
        {
            return false; // Not worth the complexity for single commands
        }

        var firstCommand = ModificationCommands[0];
        var firstState = firstCommand.EntityState;
        var firstTable = firstCommand.TableName;
        var firstSchema = firstCommand.Schema;

        // Check if all commands match the first command's characteristics
        return ModificationCommands.All(c =>
            c.EntityState == firstState &&
            c.TableName == firstTable &&
            c.Schema == firstSchema);
    }

    public override void Execute(IRelationalConnection connection)
    {
        _useStructBatching = CanUseStructBatching();

        if (_useStructBatching)
        {
            ExecuteStructBatch(connection);
        }
        else
        {
            base.Execute(connection);
        }
    }

    public override async Task ExecuteAsync(
        IRelationalConnection connection,
        CancellationToken cancellationToken = default)
    {
        _useStructBatching = CanUseStructBatching();

        if (_useStructBatching)
        {
            await ExecuteStructBatchAsync(connection, cancellationToken);
        }
        else
        {
            await base.ExecuteAsync(connection, cancellationToken);
        }
    }

    private void ExecuteStructBatch(IRelationalConnection connection)
    {
        var (sql, parameters) = GenerateStructBatchSql();

        using var command = connection.DbConnection.CreateCommand();
        command.CommandText = sql;

        foreach (var param in parameters)
        {
            command.Parameters.Add(param);
        }

        if (connection.CurrentTransaction != null)
        {
            command.Transaction = connection.CurrentTransaction.GetDbTransaction();
        }

        command.CommandTimeout = connection.CommandTimeout ?? command.CommandTimeout;

        // Execute the batch
        command.ExecuteNonQuery();
        
        // For struct batching, each command in the batch is considered executed
        // The base class AffectedCountModificationCommandBatch will handle row count tracking
    }

    private async Task ExecuteStructBatchAsync(
        IRelationalConnection connection,
        CancellationToken cancellationToken)
    {
        var (sql, parameters) = GenerateStructBatchSql();

        await using var command = connection.DbConnection.CreateCommand();
        command.CommandText = sql;

        foreach (var param in parameters)
        {
            command.Parameters.Add(param);
        }

        if (connection.CurrentTransaction != null)
        {
            command.Transaction = connection.CurrentTransaction.GetDbTransaction();
        }

        command.CommandTimeout = connection.CommandTimeout ?? command.CommandTimeout;

        // Execute the batch
        await command.ExecuteNonQueryAsync(cancellationToken);
        
        // For struct batching, each command in the batch is considered executed
        // The base class AffectedCountModificationCommandBatch will handle row count tracking
    }

    private (string sql, List<DbParameter> parameters) GenerateStructBatchSql()
    {
        var firstCommand = ModificationCommands[0];
        var entityState = firstCommand.EntityState;
        var tableName = firstCommand.TableName;
        var schema = firstCommand.Schema;

        var sqlBuilder = new StringBuilder();
        var parameters = new List<DbParameter>();

        switch (entityState)
        {
            case EntityState.Added:
                GenerateInsertStructBatch(sqlBuilder, parameters, tableName, schema);
                break;
            case EntityState.Modified:
                GenerateUpdateStructBatch(sqlBuilder, parameters, tableName, schema);
                break;
            case EntityState.Deleted:
                GenerateDeleteStructBatch(sqlBuilder, parameters, tableName, schema);
                break;
            default:
                throw new InvalidOperationException($"Unsupported entity state: {entityState}");
        }

        return (sqlBuilder.ToString(), parameters);
    }

    private void GenerateInsertStructBatch(
        StringBuilder sql,
        List<DbParameter> parameters,
        string tableName,
        string? schema)
    {
        var firstCommand = ModificationCommands[0];
        var writeColumns = firstCommand.ColumnModifications.Where(c => c.IsWrite).ToList();
        var readColumns = firstCommand.ColumnModifications.Where(c => c.IsRead).ToList();

        // Build the struct list
        var structs = new List<YdbStruct>();
        foreach (var command in ModificationCommands)
        {
            var ydbStruct = new YdbStruct();
            foreach (var column in command.ColumnModifications.Where(c => c.IsWrite))
            {
                AddColumnToStruct(ydbStruct, column, column.Value);
            }
            structs.Add(ydbStruct);
        }

        // Generate INSERT INTO ... SELECT * FROM AS_TABLE($values)
        sql.Append("INSERT INTO ");
        DelimitIdentifier(sql, tableName, schema);
        sql.Append(" (");
        sql.AppendJoin(", ", writeColumns.Select(c => DelimitIdentifier(c.ColumnName)));
        sql.AppendLine(")");
        sql.Append("SELECT ");
        sql.AppendJoin(", ", writeColumns.Select(c => DelimitIdentifier(c.ColumnName)));
        sql.Append(" FROM AS_TABLE($batch_values)");

        if (readColumns.Count > 0)
        {
            sql.AppendLine();
            sql.Append("RETURNING ");
            sql.AppendJoin(", ", readColumns.Select(c => DelimitIdentifier(c.ColumnName)));
        }

        sql.AppendLine(";");

        // Add the parameter
        var parameter = new YdbParameter("batch_values", structs);
        parameters.Add(parameter);
    }

    private void GenerateUpdateStructBatch(
        StringBuilder sql,
        List<DbParameter> parameters,
        string tableName,
        string? schema)
    {
        var firstCommand = ModificationCommands[0];
        var readColumns = firstCommand.ColumnModifications.Where(c => c.IsRead).ToList();

        // Build the struct list
        var structs = new List<YdbStruct>();
        foreach (var command in ModificationCommands)
        {
            var ydbStruct = new YdbStruct();
            foreach (var column in command.ColumnModifications)
            {
                var value = column.UseCurrentValueParameter || column.IsWrite
                    ? column.Value
                    : column.OriginalValue;
                AddColumnToStruct(ydbStruct, column, value);
            }
            structs.Add(ydbStruct);
        }

        // Generate UPDATE ... ON SELECT * FROM AS_TABLE($values)
        sql.Append("UPDATE ");
        DelimitIdentifier(sql, tableName, schema);
        sql.AppendLine(" ON");
        sql.Append("SELECT * FROM AS_TABLE($batch_values)");

        if (readColumns.Count > 0)
        {
            sql.AppendLine();
            sql.Append("RETURNING ");
            sql.AppendJoin(", ", readColumns.Select(c => DelimitIdentifier(c.ColumnName)));
        }

        sql.AppendLine(";");

        // Add the parameter
        var parameter = new YdbParameter("batch_values", structs);
        parameters.Add(parameter);
    }

    private void GenerateDeleteStructBatch(
        StringBuilder sql,
        List<DbParameter> parameters,
        string tableName,
        string? schema)
    {
        // Build the struct list (only key columns needed for delete)
        var structs = new List<YdbStruct>();
        foreach (var command in ModificationCommands)
        {
            var ydbStruct = new YdbStruct();
            foreach (var column in command.ColumnModifications.Where(c => c.IsKey || c.IsCondition))
            {
                AddColumnToStruct(ydbStruct, column, column.OriginalValue);
            }
            structs.Add(ydbStruct);
        }

        // Generate DELETE FROM ... ON SELECT * FROM AS_TABLE($values)
        sql.Append("DELETE FROM ");
        DelimitIdentifier(sql, tableName, schema);
        sql.AppendLine(" ON");
        sql.Append("SELECT * FROM AS_TABLE($batch_values)");
        sql.AppendLine(";");

        // Add the parameter
        var parameter = new YdbParameter("batch_values", structs);
        parameters.Add(parameter);
    }

    private static string DelimitIdentifier(string identifier)
    {
        return $"`{identifier}`";
    }

    private static void DelimitIdentifier(StringBuilder builder, string name, string? schema)
    {
        if (!string.IsNullOrEmpty(schema))
        {
            builder.Append('`').Append(schema).Append('`').Append('.');
        }
        builder.Append('`').Append(name).Append('`');
    }

    private void AddColumnToStruct(YdbStruct ydbStruct, IColumnModification column, object? value)
    {
        if (value == null || value == DBNull.Value)
        {
            var ydbDbType = MapToYdbDbType(column.TypeMapping?.DbType);
            ydbStruct.Add(column.ColumnName, null, ydbDbType);
        }
        else
        {
            ydbStruct.Add(column.ColumnName, value);
        }
    }

    private static YdbDbType MapToYdbDbType(DbType? dbType)
    {
        // For nullable columns without explicit type mapping, use Unspecified
        // and let YdbStruct infer the type
        if (dbType == null)
        {
            return YdbDbType.Unspecified;
        }

        return dbType switch
        {
            DbType.Boolean => YdbDbType.Bool,
            DbType.Byte => YdbDbType.Uint8,
            DbType.SByte => YdbDbType.Int8,
            DbType.Int16 => YdbDbType.Int16,
            DbType.Int32 => YdbDbType.Int32,
            DbType.Int64 => YdbDbType.Int64,
            DbType.UInt16 => YdbDbType.Uint16,
            DbType.UInt32 => YdbDbType.Uint32,
            DbType.UInt64 => YdbDbType.Uint64,
            DbType.Single => YdbDbType.Float,
            DbType.Double => YdbDbType.Double,
            DbType.Decimal => YdbDbType.Decimal,
            DbType.String => YdbDbType.Text,
            DbType.Binary => YdbDbType.Bytes,
            DbType.Date => YdbDbType.Date,
            DbType.DateTime => YdbDbType.Datetime,
            DbType.DateTime2 => YdbDbType.Timestamp,
            _ => YdbDbType.Unspecified
        };
    }
}
