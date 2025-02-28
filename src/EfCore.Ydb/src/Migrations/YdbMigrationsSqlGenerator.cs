using System;
using EfCore.Ydb.Metadata.Internal;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;
using Microsoft.EntityFrameworkCore.Migrations.Operations;

namespace EfCore.Ydb.Migrations;

public class YdbMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public YdbMigrationsSqlGenerator(MigrationsSqlGeneratorDependencies dependencies) : base(dependencies)
    {
    }

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

        builder.Append("CREATE ");
        // TODO: Support EXTERNAL tables?
        builder
            .Append("TABLE ")
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

        if (terminate)
        {
            builder.Append(";");
            EndStatement(builder, suppressTransaction: true);
        }
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
                "int32" => "SERIAL",
                "int64" => "BIGSERIAL",
                _ => throw new NotSupportedException("Serial column supported only for int32 and int64")
            };
        }

        builder
            .Append(Dependencies.SqlGenerationHelper.DelimitIdentifier(name))
            .Append(" ")
            // TODO: Add DEFAULT logic somewhere here
            .Append(columnType)
            .Append(operation.IsNullable ? " NULL" : " NOT NULL");
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

    protected override void Generate(SqlOperation operation, IModel? model, MigrationCommandListBuilder builder)
    {
        builder.AppendLine(operation.Sql);
        // TODO: Find out how to apply migration without suppressing transaction
        // We suppress transaction because CREATE/DROP operations cannot be executed during them
        EndStatement(builder, suppressTransaction: true);
    }

    private string DelimitIdentifier(string name, string? schema)
        => Dependencies.SqlGenerationHelper.DelimitIdentifier(name, schema);
}
