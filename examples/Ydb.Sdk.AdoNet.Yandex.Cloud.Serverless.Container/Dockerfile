FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore
RUN dotnet publish -c Release -r linux-x64 -o /app/build

FROM --platform=linux/amd64 mcr.microsoft.com/dotnet/runtime:8.0 AS run
WORKDIR /app
COPY --from=build /app/build/. .
ENTRYPOINT ["dotnet", "Ydb.Sdk.AdoNet.Yandex.Cloud.Serverless.Container.dll"]
