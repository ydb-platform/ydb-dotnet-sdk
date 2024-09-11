// See https://aka.ms/new-console-template for more information

using Dapper;
using Ydb.Sdk.Ado;

// Init Users table
using var connection = new YdbDataSource().OpenConnection();

connection.Execute("""
                   CREATE TABLE Users(
                       Id Int32,
                       Name Text,
                       Email Text,
                       PRIMARY KEY (Id)   
                   );
                   """);

connection.Execute("INSERT INTO Users(Id, Name, Email) VALUES ($Id, $Name, $Email)",
    new User { Id = 1, Name = "Name", Email = "Email" });

Console.WriteLine(connection.QuerySingle<User>("SELECT * FROM Users WHERE Id = $Id",
    new { Id = 1 }));

connection.Execute("DROP TABLE Users");

internal class User
{
    public int Id { get; init; }
    public string Name { get; init; } = null!;
    public string Email { get; init; } = null!;

    public override string ToString()
    {
        return $"Id: {Id}, Name: {Name}, Email: {Email}";
    }
}