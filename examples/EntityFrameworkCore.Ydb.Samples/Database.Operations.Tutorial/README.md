# Querying data using LINQ from Entity Framework Core YDB

This sample demonstrates how to query data from YDB using Entity Framework Core and LINQ.

Based on the [tutorial](https://www.csharptutorial.net/entity-framework-core-tutorial/).

## Running

1. Set up [YDB local](https://ydb.tech/docs/en/reference/docker/start).

2. Install the EF Core CLI tool and dependencies (if needed):
    ```bash
    dotnet tool install --global dotnet-ef
    dotnet add package Microsoft.EntityFrameworkCore.Design
    ```

3. Create the database and apply migrations:
    ```bash
    dotnet ef migrations add InitialCreate
    dotnet ef database update
    ```

4. Run all operations sequentially with:
    ```bash
    dotnet run
    ```

## Available Operations

You can execute individual operations by specifying the desired action as a command-line argument. Available actions:

- ReadCsv
- Select
- OrderBy
- QueryLinq
- Where
- InOperator
- Like
- InnerJoin
- GroupBy

```bash
dotnet run Select
```