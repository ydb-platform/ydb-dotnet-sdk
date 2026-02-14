FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src

COPY . .
RUN dotnet publish "examples/Ydb.Sdk.AdoNet.OpenTelemetry/Ydb.Sdk.AdoNet.OpenTelemetry.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "Ydb.Sdk.AdoNet.OpenTelemetry.dll"]
