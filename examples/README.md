# YDB .NET SDK Examples

## Prerequisites
.NET 6

## Running examples

1. Clone repository
    ```bash
    git clone https://github.com/ydb-platform/ydb-dotnet-sdk.git
    ```
   
2. Build solution
    ```bash
    cd ydb-dotnet-sdk/examples/src
    dotnet build
    ```
   
3. Run example  
   ```bash
    cd <ExampleName>
    dotnet run -e <Endpoint> -d <Database>
    ```

## Provided examples

### BasicExample
Demonstrates basic operations with YDB, including:
* Driver initialization
* Table client initialization
* Table creation via SchemeQuery (DDL)
* Data queries (OLTP) & transactions (read, modify)
* Interactive transactions
* ReadTable for streaming read of table contents
* ScanQuery for streaming wide queries (OLAP)
