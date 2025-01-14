using Microsoft.EntityFrameworkCore.Migrations;

namespace Ef.Ydb.Migrations;

public class YdbMigrationsSqlGenerator : MigrationsSqlGenerator
{
    public YdbMigrationsSqlGenerator(
        MigrationsSqlGeneratorDependencies dependencies
    ) : base(dependencies)
    {
    }
}
