using Internal;
using Microsoft.EntityFrameworkCore.Migrations;

namespace EF;

public class SloMigration : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder) => migrationBuilder.Sql(SloTable.Options);
}