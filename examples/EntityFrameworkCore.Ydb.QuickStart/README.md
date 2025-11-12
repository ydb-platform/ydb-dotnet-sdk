# Entity Framework Core YDB Quick Start

A sample application from
the [official documentation](https://learn.microsoft.com/en-us/ef/core/get-started/overview/first-app?tabs=netcore-cli)
shows how to get started with EF, define a model, populate it with data and then query the database.

## Running QuickStart

1. Setup [YDB local](https://ydb.tech/docs/en/reference/docker/start).

2. Create the database:
    ```bash
    dotnet tool install --global dotnet-ef --version 9.0.10
    dotnet add package Microsoft.EntityFrameworkCore.Design
    dotnet ef migrations add InitialCreate
    dotnet ef database update
    ```
   
3. Run the app: `dotnet run`
