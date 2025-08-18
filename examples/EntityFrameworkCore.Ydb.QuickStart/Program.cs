using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

await using var db = new BloggingContext();

var db = db.CreateExecutionDatabase();
// Create
Console.WriteLine("Inserting a new blog");
db.Add(new Blog { Url = "http://blogs.msdn.com/adonet" });
await db.SaveChangesAsync();

// Read
Console.WriteLine("Querying for a blog");
var blog = await db.Blogs
    .OrderBy(b => b.BlogId)
    .FirstAsync();

// Update
Console.WriteLine("Updating the blog and adding a post");
blog.Url = "https://devblogs.microsoft.com/dotnet";
blog.Posts.Add(
    new Post { Title = "Hello World", Content = "I wrote an app using EF Core!" });
await db.SaveChangesAsync();

// Delete
Console.WriteLine("Delete the blog");
db.Remove(blog);
await db.SaveChangesAsync();

internal class BloggingContextFactory : IDesignTimeDbContextFactory<BloggingContext>
{
    public BloggingContext CreateDbContext(string[] args)
    {
        var optionsBuilder = new DbContextOptionsBuilder<BloggingContext>();

        // IMPORTANT!
        // Disables retries for the migrations context.
        // Required because migration operations may use suppressed or explicit transactions,
        // and enabling retries in this case leads to runtime errors with this provider.
        //
        // "System.NotSupportedException: User transaction is not supported with a TransactionSuppressed migrations or a retrying execution strategy."
        //
        // Bottom line: ALWAYS disable retries for design-time/migration contexts to avoid migration failures and errors.
        return new BloggingContext(
            optionsBuilder.UseYdb("Host=localhost;Port=2136;Database=/local",
                builder => builder.DisableRetryOnFailure()
            ).Options
        );
    }
}

internal class BloggingContext : DbContext
{
    internal BloggingContext()
    {
    }

    internal BloggingContext(DbContextOptions<BloggingContext> options) : base(options)
    {
    }

    public DbSet<Blog> Blogs { get; set; }
    public DbSet<Post> Posts { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options) =>
        options.UseYdb("Host=localhost;Port=2136;Database=/local");
}

internal class Blog
{
    public int BlogId { get; init; }

    public string Url { get; set; } = string.Empty;

    // ReSharper disable once CollectionNeverQueried.Global
    public List<Post> Posts { get; init; } = [];
}

internal class Post
{
    public int PostId { get; init; }

    public string Title { get; init; } = string.Empty;

    public string Content { get; init; } = string.Empty;

    public Blog Blog { get; init; } = null!;
}