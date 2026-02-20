using System.Text.Json;
using EntityFrameworkCore.Ydb.Extensions;
using EntityFrameworkCore.Ydb.FunctionalTests.TestUtilities;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace EntityFrameworkCore.Ydb.FunctionalTests;

public class JsonTypeMappingTests
{
    private const string Json1 = """{"id":1,"nested":{"x":true},"tags":["a","b"]}""";
    private const string Json2 = """{"id":2,"nested":{"x":false},"tags":["c"]}""";

    [Fact]
    public async Task Store_type_Json_for_string_JsonElement_JsonDocument()
    {
        await using var testStore = YdbTestStoreFactory.Instance.Create(nameof(JsonTypeMappingTests) + "_Json");
        await using var db = new JsonDbContext();

        await testStore.CleanAsync(db);
        await db.Database.EnsureCreatedAsync();

        await RunTest(db);
    }

    [Fact]
    public async Task Store_type_JsonDocument_for_string_JsonElement_JsonDocument()
    {
        await using var testStore =
            YdbTestStoreFactory.Instance.Create(nameof(JsonTypeMappingTests) + "_JsonDocument");
        await using var db = new JsonDocumentDbContext();

        await testStore.CleanAsync(db);
        await db.Database.EnsureCreatedAsync();

        await RunTest(db);
    }

    private static async Task RunTest(DbContext db)
    {
        var id = Guid.NewGuid();

        // Insert
        db.Add(new ActionHandler
        {
            Id = id,
            ContextString = Json1,
            ContextElement = CreateElement(Json1),
            ContextDocument = JsonDocument.Parse(Json1)
        });

        await db.SaveChangesAsync();

        // Read
        db.ChangeTracker.Clear();

        var loaded = await db.Set<ActionHandler>().SingleAsync(x => x.Id == id);

        AssertJsonEquivalent(Json1, loaded.ContextString);
        Assert.NotNull(loaded.ContextElement);
        using (var expected1 = JsonDocument.Parse(Json1))
        {
            Assert.True(JsonElement.DeepEquals(expected1.RootElement, loaded.ContextElement!.Value));
        }

        Assert.NotNull(loaded.ContextDocument);
        using (var expected1 = JsonDocument.Parse(Json1))
        {
            Assert.True(JsonElement.DeepEquals(expected1.RootElement, loaded.ContextDocument!.RootElement));
        }

        // Update
        loaded.ContextString = Json2;
        loaded.ContextElement = CreateElement(Json2);
        loaded.ContextDocument = JsonDocument.Parse(Json2);

        await db.SaveChangesAsync();

        // Read after update
        db.ChangeTracker.Clear();

        var updated = await db.Set<ActionHandler>().SingleAsync(x => x.Id == id);

        AssertJsonEquivalent(Json2, updated.ContextString);
        Assert.NotNull(updated.ContextElement);
        using (var expected2 = JsonDocument.Parse(Json2))
        {
            Assert.True(JsonElement.DeepEquals(expected2.RootElement, updated.ContextElement!.Value));
        }

        Assert.NotNull(updated.ContextDocument);
        using (var expected2 = JsonDocument.Parse(Json2))
        {
            Assert.True(JsonElement.DeepEquals(expected2.RootElement, updated.ContextDocument!.RootElement));
        }

        // Delete
        db.Remove(updated);
        await db.SaveChangesAsync();

        Assert.Equal(0, await db.Set<ActionHandler>().CountAsync());
    }

    private static JsonElement CreateElement(string json)
        => JsonDocument.Parse(json).RootElement.Clone();

    private static void AssertJsonEquivalent(string expectedJson, string? actualJson)
    {
        Assert.NotNull(actualJson);

        using var expected = JsonDocument.Parse(expectedJson);
        using var actual = JsonDocument.Parse(actualJson);

        Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement));
    }

    private sealed class ActionHandler
    {
        public Guid Id { get; init; }
        public string? ContextString { get; set; }
        public JsonElement? ContextElement { get; set; }
        public JsonDocument? ContextDocument { get; set; }
    }

    private sealed class JsonDbContext : DbContext
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<ActionHandler>(b =>
            {
                b.ToTable("action_handlers");
                b.HasKey(x => x.Id);

                b.Property(x => x.ContextString).HasColumnType("Json");
                b.Property(x => x.ContextElement).HasColumnType("Json");
                b.Property(x => x.ContextDocument).HasColumnType("Json");
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .EnableServiceProviderCaching(false);
    }

    private sealed class JsonDocumentDbContext : DbContext
    {
        protected override void OnModelCreating(ModelBuilder modelBuilder) =>
            modelBuilder.Entity<ActionHandler>(b =>
            {
                b.ToTable("action_handlers");
                b.HasKey(x => x.Id);

                b.Property(x => x.ContextString).HasColumnType("JsonDocument");
                b.Property(x => x.ContextElement).HasColumnType("JsonDocument");
                b.Property(x => x.ContextDocument).HasColumnType("JsonDocument");
            });

        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder) => optionsBuilder
            .UseYdb("Host=localhost;Port=2136")
            .EnableServiceProviderCaching(false);
    }
}
