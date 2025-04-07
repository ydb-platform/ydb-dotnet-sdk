using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;

await using var db = new BloggingContext();

await db.Database.EnsureCreatedAsync();

Console.WriteLine("Inserting a new blog");
db.Add(new Blog { Url = "http://blogs.msdn.com/adonet" });
await db.SaveChangesAsync();

Console.WriteLine("Querying for a blog");
var blog = await db.Blogs
    .OrderBy(b => b.BlogId)
    .FirstAsync();

Console.WriteLine("Updating the blog and adding a post");
blog.Url = "https://devblogs.microsoft.com/dotnet";
blog.Posts.Add(new Post { Title = "Hello World", Content = "I wrote an app using EF Core!" });
await db.SaveChangesAsync();

Console.WriteLine("Delete the blog");
db.Remove(blog);
await db.SaveChangesAsync();

await db.Database.EnsureDeletedAsync();

internal class BloggingContext : DbContext
{
    public DbSet<Blog> Blogs { get; set; }
    public DbSet<Post> Posts { get; set; }

    protected override void OnConfiguring(DbContextOptionsBuilder options)
        => options.UseYdb("Host=localhost;Port=2136;Database=/local");
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