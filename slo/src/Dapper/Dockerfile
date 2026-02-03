FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy solution and project files
COPY ["slo/src/Dapper/AdoNet.Dapper.csproj", "slo/src/Dapper/"]
COPY ["slo/src/Internal/Internal.csproj", "slo/src/Internal/"]
COPY ["src/Ydb.Sdk/src/Ydb.Sdk.csproj", "src/Ydb.Sdk/src/"]

# Restore dependencies
RUN dotnet restore "slo/src/Dapper/AdoNet.Dapper.csproj"

# Copy source code
COPY . .

# Build
WORKDIR "/src/slo/src/Dapper"
RUN dotnet build "AdoNet.Dapper.csproj" -c Release -o /app/build

# Publish
FROM build AS publish
RUN dotnet publish "AdoNet.Dapper.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime image
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "AdoNet.Dapper.dll"]