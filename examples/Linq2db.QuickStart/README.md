# Linq2DB YDB Quick Start

A tiny sample that shows how to connect to **YDB** with **Linq2DB**, create tables, seed demo data, run parameterized queries and a transaction.

## Running QuickStart

1. **Start YDB Local**  
   Follow the official guide: https://ydb.tech/docs/en/reference/docker/start  
   Defaults expected by the sample:
   - non‑TLS port: **2136**
   - TLS port: **2135**
   - database: **/local**

2. **(Optional) Configure environment variables**  
   The app reads connection settings from env vars (safe defaults are used if missing).

   **Bash**
   ```bash
   export YDB_HOST=localhost
   export YDB_PORT=2136
   export YDB_DB=/local
   export YDB_USE_TLS=false
   # export YDB_TLS_PORT=2135
   ```

   **PowerShell**
   ```powershell
   $env:YDB_HOST="localhost"
   $env:YDB_PORT="2136"
   $env:YDB_DB="/local"
   $env:YDB_USE_TLS="false"
   # $env:YDB_TLS_PORT="2135"
   ```

3. **Restore & run**
   ```bash
   dotnet restore
   dotnet run
   ```