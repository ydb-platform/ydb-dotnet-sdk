// See https://aka.ms/new-console-template for more information

using System.ComponentModel.DataAnnotations.Schema;
using Dapper;
using Ydb.Sdk.Ado;

// Init Users table
await using var connection = await new YdbDataSource().OpenConnectionAsync();
await connection.OpenAsync();

connection.ExecuteAsync("""
                        CREATE TABLE Users(
                            Id Int32,
                            Name Text,
                            Email Text,
                            PRIMARY KEY (Id)   
                        );
                        """);

connection.ExecuteAsync("INSERT INTO Users(Id, Name, Email) VALUES ($Id, $Name, $Email)",
    new User { Id = 1, Name = "Name", Email = "Email" });

Console.WriteLine(connection.QuerySingleAsync<User>("SELECT * FROM Users WHERE Id = $Id",
    new { Id = 1 }));

await new YdbConnection().ExecuteAsync("DROP TABLE Users");

internal class User
{
    [Column("id")] public int Id { get; init; }
    [Column("name")] public string Name { get; init; } = null!;
    [Column("email")] public string Email { get; init; } = null!;

    public override string ToString()
    {
        return $"Id: {Id}, Name: {Name}, Email: {Email}";
    }
}