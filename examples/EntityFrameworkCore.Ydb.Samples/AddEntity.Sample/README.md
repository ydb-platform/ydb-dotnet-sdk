# Entity Framework Core YDB Add Entity Sample

This sample introduces the basics of using EF Core with YDB.  
It covers what EF Core is, how to create a simple .NET app with EF Core, how to define a data model, set up a database connection, and perform basic CRUD operations via the EF Core API.

Based on the [tutorial](https://www.csharptutorial.net/entity-framework-core-tutorial/getting-started-with-entity-framework-core/).

## Running QuickStart

1. Set up [YDB local](https://ydb.tech/docs/en/reference/docker/start).

2. Install the EF Core CLI tool and dependencies (if needed):
    ```bash
    dotnet tool install --global dotnet-ef --version 9.0.10
    dotnet add package Microsoft.EntityFrameworkCore.Design --version 9.0.10
    ```

3. Create the database and apply migrations:
    ```bash
    dotnet ef migrations add InitialCreate
    dotnet ef database update
    ```

4. Run the sample:
    ```bash
    dotnet run
    ```
