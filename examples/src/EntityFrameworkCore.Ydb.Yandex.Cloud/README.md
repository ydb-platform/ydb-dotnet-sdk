# Entity Framework Core YDB to Yandex Cloud Example

A sample application that defines a model, populates it with data, and then queries a database in Yandex Cloud.

## Running

```dotnet
dotnet run --connectionString "UseTls=true;Host=<host>;Port=2135;Database=<database>" --saFilePath "<path/to/SaKey.json>"
```

Required options:

1. `connectionString` - Connection string in ADO.NET format: `Key1=Value1;Key2=Value2;...`.
2. `saFilePath` - path to the service account key.
