using Internal;
using Microsoft.EntityFrameworkCore;

namespace EF;

public class TableDbContext(DbContextOptions<TableDbContext> options) : DbContext(options)
{
    public DbSet<SloTable> SloEntities { get; set; }

    protected override void OnModelCreating(ModelBuilder modelBuilder) =>
        modelBuilder.Entity<SloTable>()
            .ToTable(SloTable.Name)
            .HasKey(e => new { e.Guid, e.Id });
}