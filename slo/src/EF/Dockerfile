FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Force all projects to build only net8.0 (avoids requiring .NET 10 SDK for EFCore.Ydb)
RUN echo '<Project><PropertyGroup><TargetFrameworks>net8.0</TargetFrameworks></PropertyGroup></Project>' > Directory.Build.props

# Copy solution and project files
COPY ["slo/src/EF/EF.csproj", "slo/src/EF/"]
COPY ["slo/src/Internal/Internal.csproj", "slo/src/Internal/"]
COPY ["src/Ydb.Sdk/src/Ydb.Sdk.csproj", "src/Ydb.Sdk/src/"]
COPY ["src/EFCore.Ydb/src/EntityFrameworkCore.Ydb.csproj", "src/EFCore.Ydb/src/"]

# Restore dependencies
RUN dotnet restore "slo/src/EF/EF.csproj"

# Copy source code
COPY . .

# Build
WORKDIR "/src/slo/src/EF"
RUN dotnet build "EF.csproj" -c Release -o /app/build

# Publish
FROM build AS publish
RUN dotnet publish "EF.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime image
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "EF.dll"]
