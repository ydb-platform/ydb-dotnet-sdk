// See https://aka.ms/new-console-template for more information

using Dapper;
using Ydb.Sdk.Ado;

// Init Users table
await using var connection = await new YdbDataSource().OpenConnectionAsync();

await connection.ExecuteAsync("DROP TABLE IF EXISTS Users");
await connection.ExecuteAsync("""
                              CREATE TABLE IF NOT EXISTS Users(
                                  Id Int32,
                                  Name Text,
                                  Email Text,
                                  PRIMARY KEY (Id)   
                              );
                              """);

await connection.ExecuteAsync("INSERT INTO Users(Id, Name, Email) VALUES (@Id, @Name, @Email)",
    new User { Id = 1, Name = "Name", Email = "Email" });

Console.WriteLine(await connection.QuerySingleAsync<User>("SELECT * FROM Users WHERE Id = @Id",
    new { Id = 1 }));

internal class User
{
    public int Id { get; init; }
    public string Name { get; init; } = null!;
    public string Email { get; init; } = null!;

    public override string ToString() => $"Id: {Id}, Name: {Name}, Email: {Email}";
}