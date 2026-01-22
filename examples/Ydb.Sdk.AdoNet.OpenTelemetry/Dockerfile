FROM mcr.microsoft.com/dotnet/runtime:10.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["Ydb.Sdk.AdoNet.OpenTelemetry.csproj", "./"]
RUN dotnet restore "./Ydb.Sdk.AdoNet.OpenTelemetry.csproj"
COPY . .
RUN dotnet build "./Ydb.Sdk.AdoNet.OpenTelemetry.csproj" -c $BUILD_CONFIGURATION -o /app/build

FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./Ydb.Sdk.AdoNet.OpenTelemetry.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Ydb.Sdk.AdoNet.OpenTelemetry.dll"]
