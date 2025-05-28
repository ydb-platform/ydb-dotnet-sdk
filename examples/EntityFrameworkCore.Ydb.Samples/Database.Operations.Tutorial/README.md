# Querying data using LINQ from Entity Framework Core YDB

This sample demonstrates how to query data from YDB using Entity Framework Core and LINQ.

Based on the [tutorial](https://www.csharptutorial.net/entity-framework-core-tutorial/).

## Running

Run all operations sequentially with:

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