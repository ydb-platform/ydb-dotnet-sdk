#!/bin/bash
set -euxo pipefail

CHANGELOG=$(cat $CHANGELOG_FILE | sed -e '/^## v.*$/,$d')
if [[ -z "$CHANGELOG" ]]
then
  echo "CHANGELOG empty"
  exit 1;
fi;

TAG="v$VERSION"
echo -e "## $TAG\n" >> $CHANGELOG_FILE.tmp
cat $CHANGELOG_FILE >> $CHANGELOG_FILE.tmp
mv $CHANGELOG_FILE.tmp $CHANGELOG_FILE;
git add $CHANGELOG_FILE;
git config --global user.email "robot@umbrella";
git config --global user.name "robot";
git commit -m "Release OpenTelemetry $TAG";
git tag "otel-$TAG"
git push --tags && git push

cd src/Ydb.Sdk.OpenTelemetry/src
dotnet pack -c Release -o out /p:Version=$VERSION
gh release create "otel-$TAG" -t "otel-$TAG" --notes "$CHANGELOG"
dotnet nuget push out/Ydb.Sdk.OpenTelemetry.$VERSION.nupkg  --skip-duplicate --api-key $NUGET_TOKEN --source https://api.nuget.org/v3/index.json
