FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

# Copy solution and project files
COPY ["slo/src/AdoNet/AdoNet.csproj", "slo/src/AdoNet/"]
COPY ["slo/src/Internal/Internal.csproj", "slo/src/Internal/"]
COPY ["src/Ydb.Sdk/src/Ydb.Sdk.csproj", "src/Ydb.Sdk/src/"]

# Restore dependencies
RUN dotnet restore "slo/src/AdoNet/AdoNet.csproj"

# Copy source code
COPY . .

# Build
WORKDIR "/src/slo/src/AdoNet"
RUN dotnet build "AdoNet.csproj" -c Release -o /app/build

# Publish
FROM build AS publish
RUN dotnet publish "AdoNet.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime image
FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "slo.dll"]