FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build

COPY ../ /src
RUN ls /src


WORKDIR /src/slo/src
RUN ls
RUN dotnet restore *.sln
RUN dotnet publish *.sln -c release -o /app --no-restore -f net6.0

#####################

FROM mcr.microsoft.com/dotnet/runtime:6.0 AS run

WORKDIR /app

COPY --from=build /app ./

ENTRYPOINT ["./slo"]
