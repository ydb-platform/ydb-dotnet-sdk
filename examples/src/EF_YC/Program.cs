using CommandLine;
using EF_YC;
using EfCore.Ydb.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Ydb.Sdk.Yc;

await Parser.Default.ParseArguments<CmdOptions>(args).WithParsedAsync(async cmd =>
{
    var loggerFactory = LoggerFactory.Create(builder => builder.AddConsole().SetMinimumLevel(LogLevel.Information));
    var saProvider = new ServiceAccountProvider(saFilePath: cmd.SaFilePath, loggerFactory: loggerFactory);

    var options = new DbContextOptionsBuilder<AppDbContext>()
        .UseYdb(cmd.ConnectionString, builder =>
        {
            builder.WithCredentialsProvider(saProvider);
            builder.WithServerCertificates(YcCerts.GetYcServerCertificates());
        })
        .Options;

    await using var db = new AppDbContext(options);
    db.Database.EnsureCreated();

    db.Users.Add(new User { Name = "Alex", Email = "alex@example.com" });
    db.Users.Add(new User { Name = "Kirill", Email = "kirill@example.com" });
    db.SaveChanges();

    var users = db.Users.OrderBy(u => u.Id).ToList();
    Console.WriteLine("Users in database:");
    foreach (var user in users)
    {
        Console.WriteLine($"- {user.Id}: {user.Name} ({user.Email})");
    }
    
    // Users in database:
    // - 1: Alex (alex@example.com)
    // - 2: Kirill (kirill@example.com)
});


internal class User
{
    public int Id { get; init; }
    public string Name { get; init; } = string.Empty;
    public string Email { get; init; } = string.Empty;
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;
}

internal class AppDbContext(DbContextOptions options) : DbContext(options)
{
    public DbSet<User> Users { get; set; }
}