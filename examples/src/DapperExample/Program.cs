// See https://aka.ms/new-console-template for more information

using Dapper;
using Ydb.Sdk.Ado;

// TODO SQL Parser @Id -> $Id 

// Init Users table
await new YdbConnection().ExecuteAsync("""
                                       CREATE TABLE Users(
                                           Id Int32,
                                           Name Text,
                                           Email Text,
                                           PRIMARY KEY (Id)   
                                       );
                                       """);

await new YdbConnection().ExecuteAsync("INSERT INTO Users(Id, Name, Email) VALUES ($Id, $Name, $Email)",
    new Dictionary<string, object> { { "$Id", 1 }, { "$Name", "Name" }, { "$Email", "Email" } });

Console.WriteLine(await new YdbConnection().QuerySingleAsync<User>("SELECT * FROM Users WHERE Id = $Id",
    new Dictionary<string, object> { { "$Id", 1 } }));

Console.WriteLine(await new YdbConnection().QuerySingleAsync<User>("SELECT * FROM Users WHERE Id = $Id",
    new { Id = 1 }));

await new YdbConnection().ExecuteAsync("DROP TABLE Users");

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