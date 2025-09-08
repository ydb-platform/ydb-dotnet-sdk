using Microsoft.EntityFrameworkCore;

namespace EntityFrameworkCore.Ydb.FunctionalTests.Query;

public sealed class ParametricDecimalContext : DbContext
{
    private readonly int _p;
    private readonly int _s;

    public ParametricDecimalContext(DbContextOptions<ParametricDecimalContext> options, int p, int s)
        : base(options)
    {
        _p = p;
        _s = s;
    }

    public DbSet<ParamItem> Items => Set<ParamItem>();

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        modelBuilder.Entity<ParamItem>(b =>
        {
            b.HasKey(x => x.Id);
            b.Property(x => x.Price).HasPrecision(_p, _s);
        });
    }
}

public sealed class ParamItem
{
    public int Id { get; set; }
    public decimal Price { get; set; }
}
