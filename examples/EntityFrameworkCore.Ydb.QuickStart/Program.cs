using EntityFrameworkCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;

await using var db = new BloggingContext();

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
// Console.WriteLine("Delete the blog");
// db.Remove(blog);
// await db.SaveChangesAsync();

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
    
    // public DbSet<IdentityUser> Users { get; set; }

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

    public bool EmailConfirmed { get; init; }

    public Blog Blog { get; init; } = null!;
}
//
// public class IdentityUser
// {
//     public virtual Guid? Id { get; protected set; }
//
//     public virtual Guid? TenantId { get; protected set; }
//
//     /// <summary>
//     /// Gets or sets the user name for this user.
//     /// </summary>
//     public virtual string UserName { get; protected internal set; }
//
//
//     public virtual string NormalizedUserName { get; protected internal set; }
//
//     public virtual string Name { get; set; }
//
//     public virtual string Surname { get; set; }
//
//     public virtual string Email { get; protected internal set; }
//
//     public virtual string NormalizedEmail { get; protected internal set; }
//
//     /// <summary>
//     /// Gets or sets a flag indicating if a user has confirmed their email address.
//     /// </summary>
//     /// <value>True if the email address has been confirmed, otherwise false.</value>
//     public virtual bool EmailConfirmed { get; protected internal set; }
//
//     public virtual string PasswordHash { get; protected internal set; }
//
//     public virtual string SecurityStamp { get; protected internal set; }
//
//     public virtual bool IsExternal { get; set; }
//
//     public virtual string PhoneNumber { get; protected internal set; }
//
//     /// <summary>
//     /// Gets or sets a flag indicating if a user has confirmed their telephone address.
//     /// </summary>
//     /// <value>True if the telephone number has been confirmed, otherwise false.</value>
//     public virtual bool PhoneNumberConfirmed { get; protected internal set; }
//
//     /// <summary>
//     /// Gets or sets a flag indicating if the user is active.
//     /// </summary>
//     public virtual bool IsActive { get; protected internal set; }
//
//     /// <summary>
//     /// Gets or sets a flag indicating if two factor authentication is enabled for this user.
//     /// </summary>
//     /// <value>True if 2fa is enabled, otherwise false.</value>
//     public virtual bool TwoFactorEnabled { get; protected internal set; }
//
//     /// <summary>
//     /// Gets or sets the date and time, in UTC, when any user lockout ends.
//     /// </summary>
//     /// <remarks>
//     /// A value in the past means the user is not locked out.
//     /// </remarks>
//     public virtual DateTimeOffset? LockoutEnd { get; protected internal set; }
//
//     /// <summary>
//     /// Gets or sets a flag indicating if the user could be locked out.
//     /// </summary>
//     /// <value>True if the user could be locked out, otherwise false.</value>
//     public virtual bool LockoutEnabled { get; protected internal set; }
//
//     /// <summary>
//     /// Gets or sets the number of failed login attempts for the current user.
//     /// </summary>
//     public virtual int AccessFailedCount { get; protected internal set; }
//
//     /// <summary>
//     /// Should change password on next login.
//     /// </summary>
//     public virtual bool ShouldChangePasswordOnNextLogin { get; protected internal set; }
//
//     /// <summary>
//     /// A version value that is increased whenever the entity is changed.
//     /// </summary>
//     public virtual int EntityVersion { get; protected set; }
//
//     /// <summary>
//     /// Gets or sets the last password change time for the user.
//     /// </summary>
//     public virtual DateTimeOffset? LastPasswordChangeTime { get; protected set; }
//
//     protected IdentityUser()
//     {
//     }
//
//     public IdentityUser(
//         Guid id,
//         string userName,
//         string email,
//         Guid? tenantId = null)
//
//     {
//         Id = id;
//         TenantId = tenantId;
//         UserName = userName;
//         NormalizedUserName = userName.ToUpperInvariant();
//         Email = email;
//         NormalizedEmail = email.ToUpperInvariant();
//         SecurityStamp = Guid.NewGuid().ToString();
//         IsActive = true;
//     }
//
//
//     /// <summary>
//     /// Use <see cref="IdentityUserManager.ConfirmEmailAsync"/> for regular email confirmation.
//     /// Using this skips the confirmation process and directly sets the <see cref="EmailConfirmed"/>.
//     /// </summary>
//     public virtual void SetEmailConfirmed(bool confirmed)
//     {
//         EmailConfirmed = confirmed;
//     }
//
//     public virtual void SetPhoneNumberConfirmed(bool confirmed)
//     {
//         PhoneNumberConfirmed = confirmed;
//     }
//
//     /// <summary>
//     /// Normally use <see cref="IdentityUserManager.ChangePhoneNumberAsync"/> to change the phone number
//     /// in the application code.
//     /// This method is to directly set it with a confirmation information.
//     /// </summary>
//     /// <param name="phoneNumber"></param>
//     /// <param name="confirmed"></param>
//     /// <exception cref="NotImplementedException"></exception>
//     public void SetPhoneNumber(string phoneNumber, bool confirmed)
//     {
//         PhoneNumber = phoneNumber;
//         PhoneNumberConfirmed = confirmed;
//     }
//
//     public virtual void SetIsActive(bool isActive)
//     {
//         IsActive = isActive;
//     }
//
//     public virtual void SetShouldChangePasswordOnNextLogin(bool shouldChangePasswordOnNextLogin)
//     {
//         ShouldChangePasswordOnNextLogin = shouldChangePasswordOnNextLogin;
//     }
//
//     public virtual void SetLastPasswordChangeTime(DateTimeOffset? lastPasswordChangeTime)
//     {
//         LastPasswordChangeTime = lastPasswordChangeTime;
//     }
//
//     public override string ToString()
//     {
//         return $"{base.ToString()}, UserName = {UserName}";
//     }
// }