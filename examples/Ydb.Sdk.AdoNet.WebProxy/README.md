# YDB Sdk ADO.NET — HTTP WebProxy

Connect to YDB through an HTTP proxy via `YdbConnectionStringBuilder.Proxy`
(`System.Net.WebProxy` / `IWebProxy`). Settings are hardcoded in `Program.cs`.

> .NET cannot speak cleartext HTTP/2 (h2c, port `2136`) through an HTTP CONNECT proxy.
> The example uses TLS on port `2135`.

## Local run

### 1. YDB Local

```bash
docker run -d --rm --name ydb-local -h localhost \
  -p 2135:2135 -p 2136:2136 -p 8765:8765 \
  -e GRPC_TLS_PORT=2135 -e GRPC_PORT=2136 -e MON_PORT=8765 \
  ydbplatform/local-ydb:latest
```

### 2. CA certificate

```bash
docker cp ydb-local:/ydb_certs/ca.pem ~/ca.pem
```

### 3. Squid (CONNECT + basic auth)

`squid.conf` allows `CONNECT` to YDB gRPC ports and requires credentials from
`passwd` (`proxy-user` / `proxy-pass`).

**Linux** (prefer `--network host`):

```bash
cd examples/Ydb.Sdk.AdoNet.WebProxy

docker rm -f ydb-proxy 2>/dev/null
docker run -d --rm --name ydb-proxy --network host \
  -v "$(pwd)/squid.conf:/etc/squid/squid.conf:ro" \
  -v "$(pwd)/passwd:/etc/squid/passwd:ro" \
  ubuntu/squid:latest
```

**macOS / Docker Desktop** (host networking is unavailable):

```bash
docker rm -f ydb-proxy 2>/dev/null
docker run -d --rm --name ydb-proxy -p 3128:3128 \
  --add-host=host.docker.internal:host-gateway \
  -v "$(pwd)/squid.conf:/etc/squid/squid.conf:ro" \
  -v "$(pwd)/passwd:/etc/squid/passwd:ro" \
  ubuntu/squid:latest
```

### 4. Run the example

```bash
dotnet run
```

`Program.cs` picks the host automatically: `localhost` on Linux,
`host.docker.internal` on macOS/Windows.

On success:

```text
message=Hello from YDB through HTTP proxy!, ts=...
Connection through proxy succeeded
```

## Hardcoded settings

- proxy: `http://127.0.0.1:3128`
- proxy credentials: `proxy-user` / `proxy-pass`
- YDB: port `2135`, `UseTls=true`, `RootCertificate=~/ca.pem`, `DisableDiscovery=true`
