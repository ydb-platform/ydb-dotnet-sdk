using System;
using System.Text;
using EfCore.Ydb.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

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
        EndStatement(builder, suppressTransaction: true);
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

        builder
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
            .Append(" ")
            // TODO: Add DEFAULT logic somewhere here
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
        EndStatement(builder);
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
            EndStatement(builder, suppressTransaction: false);
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
        EndStatement(builder, suppressTransaction: false);
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
        EndStatement(builder, suppressTransaction: false);
    }

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

    protected override void CreateTableUniqueConstraints(CreateTableOperation operation, IModel? model,
        MigrationCommandListBuilder builder)
    {
        // We don't have unique constraints
    }

    protected override void UniqueConstraint(AddUniqueConstraintOperation operation, IModel? model,
        MigrationCommandListBuilder builder)
    {
        // Same comment about Unique constraints
    }

    protected override void Generate(
        CreateIndexOperation operation,
        IModel? model,
        MigrationCommandListBuilder builder,
        bool terminate = true
    )
    {
        // TODO: We do have Indexes!
        // But they're not implemented yet. Ignoring indexes because otherwise table generation during tests will fail
    }


    // ReSharper disable once RedundantOverriddenMember
    protected override void EndStatement(
        MigrationCommandListBuilder builder,
        // ReSharper disable once OptionalParameterHierarchyMismatch
        bool suppressTransaction = true
    ) => base.EndStatement(builder, suppressTransaction);

    private string DelimitIdentifier(string name, string? schema)
        => Dependencies.SqlGenerationHelper.DelimitIdentifier(name, schema);
}
