#!/bin/bash
set -euxo pipefail

CHANGELOG_VERSION=$(grep -m 1 '^## v' $CHANGELOG_FILE | sed 's/## v//')
if [[ "$CHANGELOG_VERSION" != "$VERSION" ]]
then
  echo "CHANGELOG version ($CHANGELOG_VERSION) does not match input version ($VERSION)"
  exit 1;
fi;

cd src/Ydb.Sdk.OpenTelemetry/src
dotnet pack -c Release -o out /p:Version=$VERSION
dotnet nuget push out/Ydb.Sdk.OpenTelemetry.$VERSION.nupkg  --skip-duplicate --api-key $NUGET_TOKEN --source https://api.nuget.org/v3/index.json
