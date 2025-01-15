using Microsoft.EntityFrameworkCore.Migrations;

namespace EfCore.Ydb.Migrations;

public class YdbMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public YdbMigrationsSqlGenerator(
        MigrationsSqlGeneratorDependencies dependencies
    ) : base(dependencies)
    {
    }
}
