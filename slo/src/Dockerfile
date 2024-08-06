FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG SRC_PATH
COPY ../ /src
RUN ls /src
WORKDIR /src/slo/src/${SRC_PATH}
RUN ls
RUN dotnet restore *.sln
RUN dotnet publish *.sln -c release -o /app --no-restore -f net8.0

#####################

FROM mcr.microsoft.com/dotnet/runtime:8.0 AS run
WORKDIR /app
COPY --from=build /app ./
ENTRYPOINT ["./slo"]
