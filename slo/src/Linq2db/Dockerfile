FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy solution and project files
COPY ["slo/src/Linq2db/Linq2db.Slo.csproj", "slo/src/Linq2db/"]
COPY ["slo/src/Internal/Internal.csproj", "slo/src/Internal/"]
COPY ["src/Ydb.Sdk/src/Ydb.Sdk.csproj", "src/Ydb.Sdk/src/"]

# Restore dependencies
RUN dotnet restore "slo/src/Linq2db.Slo/Linq2db.Slo.csproj"

# Copy source code
COPY . .

# Build
WORKDIR "/src/slo/src/Linq2db"
RUN dotnet build "Linq2db.Slo.csproj" -c Release -o /app/build

# Publish
FROM build AS publish
RUN dotnet publish "Linq2db.Slo.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime image
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Linq2db.Slo.dll"]