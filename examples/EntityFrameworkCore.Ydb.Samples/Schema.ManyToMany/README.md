# Entity Framework Core YDB Schema Many-to-Many Example

This sample demonstrates how to model a [many-to-many relationship](https://www.csharptutorial.net/entity-framework-core-tutorial/ef-core-many-to-many-relationships/) in Entity Framework Core targeting YDB.

The focus here is to show what database schema (DDL) will be generated by EF Core migrations for such a model.

## How to View the Generated Schema

1. Set up a local YDB instance:  
   [YDB local setup guide](https://ydb.tech/docs/en/reference/docker/start)

2. Install the EF Core CLI tool (if needed):
    ```bash
    dotnet tool install --global dotnet-ef
    dotnet add package Microsoft.EntityFrameworkCore.Design
    ```

3. Add a migration to generate the DDL (if not already present):
    ```bash
    dotnet ef migrations add InitialCreate
    ```

4. Generate the SQL script for the schema:
    ```bash
    dotnet ef migrations script
    ```

5. Inspect the generated SQL script to see how EF Core maps the many-to-many relationship in YDB.
