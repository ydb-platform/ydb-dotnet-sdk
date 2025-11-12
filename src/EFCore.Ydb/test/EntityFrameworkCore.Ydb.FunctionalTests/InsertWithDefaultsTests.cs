using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Migrations;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class InsertWithDefaultsTests
{
    [Fact]
    public async Task Insert_WritesDefaultedNonNullColumns_Succeeds()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create("InsertWithDefaultsTests");

        await using var dbContext = new TestEntityDbContext();
        await testStore.CleanAsync(dbContext);

        await dbContext.Database.MigrateAsync();

        dbContext.Entities.AddRange(new TestEntity { Id = 1 }, new TestEntity { Id = 2 }, new TestEntity { Id = 3 });
        await dbContext.SaveChangesAsync();

        foreach (var entity in dbContext.Entities.ToList())
        {
            Assert.True(entity.BoolValue);
            Assert.Equal(1, entity.Int8Value);
            Assert.Equal(1, entity.Int16Value);
            Assert.Equal(1, entity.Int32Value);
            Assert.Equal(1, entity.Int64Value);

            Assert.Equal(1, entity.Uint8Value);
            Assert.Equal(1, entity.Uint16Value);
            Assert.Equal(1u, entity.Uint32Value);
            Assert.Equal(1u, entity.Uint64Value);

            Assert.Equal(1, entity.FloatValue);
            Assert.Equal(1, entity.DoubleValue);
            Assert.Equal(1.00000m, entity.DecimalValue);

            Assert.Equal(Guid.Empty, entity.GuidValue);

            Assert.Equal("text", entity.TextValue);
            Assert.Empty(entity.BytesValue);

            Assert.Equal(new DateOnly(1971, 12, 1), entity.DateValue);
            Assert.Equal(new DateTime(1971, 12, 1, 0, 0, 0, DateTimeKind.Utc), entity.DateTimeValue);
            Assert.Equal(TimeSpan.FromSeconds(1), entity.IntervalValue);
        }
    }

    public class TestEntityDbContext : DbContext
    {
        public DbSet<TestEntity> Entities => Set<TestEntity>();

        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<TestEntity>(e =>
            {
                e.ToTable("TestEntity");
                e.HasKey(x => x.Id);
                e.Property(x => x.BoolValue).HasDefaultValue(true);
                e.Property(x => x.Int8Value).HasDefaultValue(1);
                e.Property(x => x.Int16Value).HasDefaultValue(1);
                e.Property(x => x.Int32Value).HasDefaultValue(1);
                e.Property(x => x.Int64Value).HasDefaultValue(1);
                e.Property(x => x.Uint8Value).HasDefaultValue(1);
                e.Property(x => x.Uint16Value).HasDefaultValue(1);
                e.Property(x => x.Uint32Value).HasDefaultValue(1);
                e.Property(x => x.Uint64Value).HasDefaultValue(1);
                e.Property(x => x.FloatValue).HasDefaultValue(1);
                e.Property(x => x.DoubleValue).HasDefaultValue(1);
                e.Property(x => x.DecimalValue).HasDefaultValue(1);
                e.Property(x => x.GuidValue).HasDefaultValue(Guid.Empty);
                e.Property(x => x.TextValue).HasDefaultValue("text");
                e.Property(x => x.BytesValue).HasDefaultValue(Array.Empty<byte>());
                e.Property(x => x.DateValue).HasDefaultValue(new DateOnly(1971, 12, 1));
                e.Property(x => x.DateTimeValue).HasDefaultValue(new DateTime(1971, 12, 1, 0, 0, 0, DateTimeKind.Utc));
                e.Property(x => x.IntervalValue).HasDefaultValue(TimeSpan.FromSeconds(1));
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136", builder => builder
                .DisableRetryOnFailure()
                .MigrationsAssembly(typeof(TestEntityMigration).Assembly.FullName))
            .EnableServiceProviderCaching(false)
            .LogTo(Console.WriteLine);
    }

    public class TestEntity
    {
        public int Id { get; set; }

        public bool? BoolValue { get; set; }

        public sbyte Int8Value { get; set; }
        public short Int16Value { get; set; }
        public int Int32Value { get; set; }
        public long Int64Value { get; set; }

        public byte Uint8Value { get; set; }
        public ushort Uint16Value { get; set; }
        public uint Uint32Value { get; set; }
        public ulong Uint64Value { get; set; }

        public float FloatValue { get; set; }
        public double DoubleValue { get; set; }
        [Precision(25, 5)] public decimal DecimalValue { get; set; }

        public Guid GuidValue { get; set; }

        public string TextValue { get; set; } = null!;
        public byte[] BytesValue { get; set; } = null!;

        public DateOnly DateValue { get; set; }
        public DateTime DateTimeValue { get; set; }
        public TimeSpan IntervalValue { get; set; }
    }

    [DbContext(typeof(TestEntityDbContext))]
    [Migration("InsertWithDefaultsTests_TestEntity")]
    private class TestEntityMigration : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder) =>
            migrationBuilder.CreateTable(
                name: "TestEntity",
                columns: table => new
                {
                    Id = table.Column<int>(type: "Int32", nullable: false),

                    BoolValue = table.Column<bool>(type: "Bool", nullable: false, defaultValue: true),

                    Int8Value = table.Column<sbyte>(type: "Int8", nullable: false, defaultValue: (sbyte)1),
                    Int16Value = table.Column<short>(type: "Int16", nullable: false, defaultValue: (short)1),
                    Int32Value = table.Column<int>(type: "Int32", nullable: false, defaultValue: 1),
                    Int64Value = table.Column<long>(type: "Int64", nullable: false, defaultValue: 1L),

                    Uint8Value = table.Column<byte>(type: "Uint8", nullable: false, defaultValue: (byte)1),
                    Uint16Value = table.Column<ushort>(type: "Uint16", nullable: false, defaultValue: (ushort)1),
                    Uint32Value = table.Column<uint>(type: "Uint32", nullable: false, defaultValue: (uint)1),
                    Uint64Value = table.Column<ulong>(type: "Uint64", nullable: false, defaultValue: (ulong)1),

                    FloatValue = table.Column<float>(type: "Float", nullable: false, defaultValue: 1f),
                    DoubleValue = table.Column<double>(type: "Double", nullable: false, defaultValue: 1d),

                    DecimalValue = table.Column<decimal>(type: "Decimal(25, 5)", precision: 25, scale: 5,
                        nullable: false, defaultValue: 1.0m),

                    GuidValue = table.Column<Guid>(type: "Uuid", nullable: false, defaultValue: Guid.Empty),

                    TextValue = table.Column<string>(type: "Text", nullable: false, defaultValue: "text"),
                    BytesValue =
                        table.Column<byte[]>(type: "Bytes", nullable: false, defaultValue: Array.Empty<byte>()),

                    DateValue = table.Column<DateOnly>(type: "Date", nullable: false,
                        defaultValue: new DateOnly(1971, 12, 1)),
                    DateTimeValue = table.Column<DateTime>(type: "Timestamp", nullable: false,
                        defaultValue: new DateTime(1971, 12, 1, 0, 0, 0, DateTimeKind.Utc)),
                    IntervalValue = table.Column<TimeSpan>(type: "Interval", nullable: false,
                        defaultValue: TimeSpan.FromSeconds(1))
                },
                constraints: table => { table.PrimaryKey("PK_TestEntity", x => x.Id); }
            );

        protected override void Down(MigrationBuilder migrationBuilder) =>
            migrationBuilder.DropTable(name: "TestEntity");
    }
}
