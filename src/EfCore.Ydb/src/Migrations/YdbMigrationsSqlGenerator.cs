using System;
using System.Text;
using EfCore.Ydb.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;
using Ydb.Sdk.Ado;

namespace EfCore.Ydb.Migrations;

public class YdbMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies)
    : MigrationsSqlGenerator(dependencies)
{
    protected override void Generate(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        if (!terminate && operation.Comment is not null)
        {
            // TODO: Handle comments
        }

        builder.Append("CREATE TABLE ")
            .Append(DelimitIdentifier(operation.Name, operation.Schema))
            .AppendLine(" (");

        using (builder.Indent())
        {
            CreateTableColumns(operation, model, builder);
            CreateTableConstraints(operation, model, builder);
            builder.AppendLine();
        }

        builder.Append(")");

        // TODO: Support `WITH {}` block

        if (!terminate)
        {
            return;
        }

        builder.Append(";");
        EndStatementSuppressTransaction(builder);
    }

    protected override void ColumnDefinition(
        string? schema,
        string table,
        string name,
        ColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder
    )
    {
        var columnType = operation.ColumnType ?? GetColumnType(schema, table, name, operation, model)!;
        var autoincrement = operation[YdbAnnotationNames.Serial] as bool?;

        if (autoincrement == true)
        {
            columnType = columnType.ToLower() switch
            {
                "int8" or "int16" => "SmallSerial",
                "int32" => "Serial",
                "int64" => "Bigserial",
                _ => throw new NotSupportedException($"Serial column isn't supported for {columnType} type")
            };
        }

        if (operation.ComputedColumnSql is not null)
        {
            throw new NotSupportedException("Computed/generated columns aren't supported in YDB");
        }

        builder
            .Append(DelimitIdentifier(name))
            .Append(" ")
            .Append(columnType)
            .Append(operation.IsNullable ? string.Empty : " NOT NULL");
    }

    protected override void CreateTablePrimaryKeyConstraint(
        CreateTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder
    )
    {
        if (operation.PrimaryKey == null) return;

        builder.AppendLine(",");
        PrimaryKeyConstraint(operation.PrimaryKey, model, builder);
    }

    protected override void PrimaryKeyConstraint(
        AddPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder
    )
    {
        builder
            .Append("PRIMARY KEY (")
            .Append(ColumnList(operation.Columns))
            .Append(")");
        IndexOptions(operation, model, builder);
    }

    protected override void Generate(RenameTableOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        if (operation.NewSchema is not null && operation.NewSchema != operation.Schema)
        {
            throw new NotSupportedException("Rename table with schema is not supported");
        }

        if (operation.NewName is null || operation.NewName == operation.Name)
        {
            return;
        }

        builder
            .Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Name, operation.Schema))
            .Append(" RENAME TO ")
            .Append(DelimitIdentifier(operation.NewName, operation.Schema))
            .AppendLine(";");
        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(
        InsertDataOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        var sqlBuilder = new StringBuilder();
        foreach (var modificationCommand in GenerateModificationCommands(operation, model))
        {
            SqlGenerator.AppendInsertOperation(sqlBuilder, modificationCommand, 0);
        }

        builder.Append(sqlBuilder.ToString());

        if (terminate)
        {
            EndStatement(builder);
        }
    }

    protected override void Generate(DeleteDataOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var sqlBuilder = new StringBuilder();
        foreach (var modificationCommand in GenerateModificationCommands(operation, model))
        {
            SqlGenerator.AppendDeleteOperation(
                sqlBuilder,
                modificationCommand,
                0);
        }

        builder.Append(sqlBuilder.ToString());
        EndStatement(builder);
    }

    protected override void Generate(
        DropTableOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder.Append("DROP TABLE ")
            .Append(DelimitIdentifier(operation.Name, operation.Schema));
        if (!terminate)
            return;
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(
        AddColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        if (operation["Relational:ColumnOrder"] != null)
            Dependencies.MigrationsLogger.ColumnOrderIgnoredWarning(operation);
        builder.Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ADD ");
        ColumnDefinition(operation, model, builder);
        if (!terminate)
            return;
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(
        DropColumnOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        builder.Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" DROP COLUMN ")
            .Append(DelimitIdentifier(operation.Name));

        if (!terminate)
            return;
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        builder.Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" ADD INDEX ")
            .Append(DelimitIdentifier(operation.Name))
            .Append(" GLOBAL ");

        if (operation.IsUnique)
        {
            builder.Append(" UNIQUE ");
        }

        if (operation.IsDescending != null)
        {
            throw new NotSupportedException("Descending columns in the index aren't supported in YDB");
        }

        builder.Append("SYNC ON (")
            .Append(ColumnList(operation.Columns))
            .Append(")");

        if (!terminate)
            return;
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(
        DropIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true)
    {
        if (operation.Table == null)
        {
            throw new YdbException("Table name must be specified for DROP INDEX in YDB");
        }

        builder.Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" DROP INDEX ")
            .Append(DelimitIdentifier(operation.Name));

        if (!terminate)
            return;
        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(
        RenameIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder
    )
    {
        if (operation.Table == null)
        {
            throw new YdbException("Table name must be specified for RENAME INDEX in YDB");
        }

        builder.Append("ALTER TABLE ")
            .Append(DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" RENAME INDEX ")
            .Append(DelimitIdentifier(operation.Name))
            .Append(" TO ")
            .Append(DelimitIdentifier(operation.NewName));

        builder.AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);
        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(UpdateDataOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        var sqlBuilder = new StringBuilder();
        foreach (var modificationCommand in GenerateModificationCommands(operation, model))
        {
            SqlGenerator.AppendUpdateOperation(
                sqlBuilder,
                modificationCommand,
                0);
        }

        builder.Append(sqlBuilder.ToString());
        EndStatement(builder);
    }

    protected override void Generate(
        DropUniqueConstraintOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder
    )
    {
        builder.Append("ALTER TABLE ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Table, operation.Schema))
            .Append(" DROP INDEX ")
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
            .AppendLine(Dependencies.SqlGenerationHelper.StatementTerminator);

        EndStatementSuppressTransaction(builder);
    }

    protected override void Generate(
        DropCheckConstraintOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder
    ) => throw new NotSupportedException("Drop check constraint isn't supported in YDB");

    protected override void Generate(
        DropForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        // Ignore bc YDB doesn't have foreign keys
    }

    protected override void Generate(
        DropPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        // Ignore bc YDB automatically drops primary keys
    }

    protected override void Generate(
        AddForeignKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        // Ignore bc YDB doesn't have foreign keys
    }

    protected override void Generate(
        AddPrimaryKeyOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        // Ignore bc YDB doesn't support adding keys outside table creation
    }

    protected override void CreateTableForeignKeys(CreateTableOperation operation, IModel? model,
        MigrationCommandListBuilder builder)
    {
        // Same comment about Foreign keys
    }

    protected override void ForeignKeyAction(ReferentialAction referentialAction, MigrationCommandListBuilder builder)
    {
        // Same comment about Foreign keys
    }

    protected override void ForeignKeyConstraint(AddForeignKeyOperation operation, IModel? model,
        MigrationCommandListBuilder builder)
    {
        // Same comment about Foreign keys
    }

    protected override void UniqueConstraint(
        AddUniqueConstraintOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder
    ) => builder
        .Append("INDEX ")
        .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(operation.Name))
        .Append(" GLOBAL UNIQUE SYNC ON (")
        .Append(ColumnList(operation.Columns))
        .Append(")");

    private void EndStatementSuppressTransaction(MigrationCommandListBuilder builder) =>
        base.EndStatement(builder, true);

    private string DelimitIdentifier(string name, string? schema = null)
        => Dependencies.SqlGenerationHelper.DelimitIdentifier(name, schema);
}
