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
