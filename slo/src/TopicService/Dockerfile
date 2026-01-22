FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

# Copy solution and project files
COPY ["slo/src/TopicService/TopicService.csproj", "slo/src/TopicService/"]
COPY ["slo/src/Internal/Internal.csproj", "slo/src/Internal/"]
COPY ["src/Ydb.Sdk/src/Ydb.Sdk.csproj", "src/Ydb.Sdk/src/"]

# Restore dependencies
RUN dotnet restore "slo/src/TopicService/TopicService.csproj"

# Copy source code
COPY . .

# Build
WORKDIR "/src/slo/src/TopicService"
RUN dotnet build "TopicService.csproj" -c Release -o /app/build

# Publish
FROM build AS publish
RUN dotnet publish "TopicService.csproj" -c Release -o /app/publish /p:UseAppHost=false

# Runtime image
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "slo.dll"]
