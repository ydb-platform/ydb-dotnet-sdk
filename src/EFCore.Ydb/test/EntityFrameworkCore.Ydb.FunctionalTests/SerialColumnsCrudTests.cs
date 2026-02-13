using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class SerialColumnsCrudTests
{
    [Fact]
    public async Task Serial_columns_basic_crud_works()
    {
        var tableSuffix = Guid.NewGuid().ToString("N");
        await using var dbContext = new SerialColumnsDbContext(tableSuffix);
        var createBigTableSql = $"""
                                 CREATE TABLE `{dbContext.BigTableName}` (
                                     `Id` Bigserial NOT NULL,
                                     `Name` Text NOT NULL,
                                     PRIMARY KEY (`Id`)
                                 );
                                 """;
        var createSerialTableSql = $"""
                                    CREATE TABLE `{dbContext.SerialTableName}` (
                                        `Id` Serial NOT NULL,
                                        `Name` Text NOT NULL,
                                        PRIMARY KEY (`Id`)
                                    );
                                    """;
        var createSmallSerialTableSql = $"""
                                         CREATE TABLE `{dbContext.SmallSerialTableName}` (
                                             `Id` SmallSerial NOT NULL,
                                             `Name` Text NOT NULL,
                                             PRIMARY KEY (`Id`)
                                         );
                                         """;
        await dbContext.Database.ExecuteSqlRawAsync(createBigTableSql);
        await dbContext.Database.ExecuteSqlRawAsync(createSerialTableSql);
        await dbContext.Database.ExecuteSqlRawAsync(createSmallSerialTableSql);

        dbContext.BigSerialEntities.Add(new BigSerialEntity { Name = "big" });
        dbContext.SerialEntities.Add(new SerialEntity { Name = "serial" });
        dbContext.SmallSerialEntities.Add(new SmallSerialEntity { Name = "small" });
        await dbContext.SaveChangesAsync();

        var big = await dbContext.BigSerialEntities.SingleAsync();
        var serial = await dbContext.SerialEntities.SingleAsync();
        var small = await dbContext.SmallSerialEntities.SingleAsync();

        Assert.True(big.Id > 0);
        Assert.True(serial.Id > 0);
        Assert.True(small.Id > 0);

        big.Name = "big-updated";
        serial.Name = "serial-updated";
        small.Name = "small-updated";
        await dbContext.SaveChangesAsync();

        Assert.Equal("big-updated", (await dbContext.BigSerialEntities.SingleAsync()).Name);
        Assert.Equal("serial-updated", (await dbContext.SerialEntities.SingleAsync()).Name);
        Assert.Equal("small-updated", (await dbContext.SmallSerialEntities.SingleAsync()).Name);

        dbContext.RemoveRange(big, serial, small);
        await dbContext.SaveChangesAsync();

        Assert.Empty(await dbContext.BigSerialEntities.ToListAsync());
        Assert.Empty(await dbContext.SerialEntities.ToListAsync());
        Assert.Empty(await dbContext.SmallSerialEntities.ToListAsync());
    }

    private sealed class SerialColumnsDbContext(string tableSuffix) : DbContext
    {
        public DbSet<BigSerialEntity> BigSerialEntities => Set<BigSerialEntity>();
        public DbSet<SerialEntity> SerialEntities => Set<SerialEntity>();
        public DbSet<SmallSerialEntity> SmallSerialEntities => Set<SmallSerialEntity>();
        public string BigTableName { get; } = $"BigSerialEntities_{tableSuffix}";
        public string SerialTableName { get; } = $"SerialEntities_{tableSuffix}";
        public string SmallSerialTableName { get; } = $"SmallSerialEntities_{tableSuffix}";

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136", builder => builder.DisableRetryOnFailure())
            .EnableServiceProviderCaching(false);

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<BigSerialEntity>(entity =>
            {
                entity.ToTable(BigTableName);
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedOnAdd();
            });

            modelBuilder.Entity<SerialEntity>(entity =>
            {
                entity.ToTable(SerialTableName);
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedOnAdd();
            });

            modelBuilder.Entity<SmallSerialEntity>(entity =>
            {
                entity.ToTable(SmallSerialTableName);
                entity.HasKey(x => x.Id);
                entity.Property(x => x.Id).ValueGeneratedOnAdd();
            });
        }
    }

    private class BigSerialEntity
    {
        public long Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class SerialEntity
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class SmallSerialEntity
    {
        public short Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
