#!/bin/bash
set -euxo pipefail

CHANGELOG=$(cat $CHANGELOG_FILE | sed -e '/^## v.*$/,$d')
if [[ -z "$CHANGELOG" ]]
then
  echo "CHANGELOG empty"
  exit 1;
fi;

MAJOR=$(cat $VERSION_FILE | grep Major | grep -Eo '[0-9]*');
MINOR=$(cat $VERSION_FILE | grep Minor | grep -Eo '[0-9]*');
PATCH=$(cat $VERSION_FILE | grep Patch | grep -Eo '[0-9]*');

VERSION="$MAJOR.$MINOR.$PATCH"

LAST_TAG="v$MAJOR.$MINOR.$PATCH";
if [ "$VERSION_CHANGE" = "MINOR" ]
then
  MINOR=$((MINOR+1));
  PATCH=0;
fi;
if [ "$VERSION_CHANGE" = "PATCH" ]
then
  PATCH=$((PATCH+1));
fi;
if [ "$RELEASE_CANDIDATE" = true ]
then
  RC=$(git tag | grep "v$MAJOR.$MINOR.$PATCH-rc" | wc -l || true);
  TAG="v$MAJOR.$MINOR.$PATCH-rc$RC";
else
  sed -e "s/Minor = [0-9]*/Minor = $MINOR/g" -i $VERSION_FILE
  sed -e "s/Patch = [0-9]*/Patch = $PATCH/g" -i $VERSION_FILE
  git add $VERSION_FILE;
  TAG="v$MAJOR.$MINOR.$PATCH";
fi;
echo "## v$TAG" >> $CHANGELOG_FILE.tmp
cat $CHANGELOG_FILE >> $CHANGELOG_FILE.tmp
mv $CHANGELOG_FILE.tmp $CHANGELOG_FILE;
git add $CHANGELOG_FILE;
git config --global user.email "robot@umbrella";
git config --global user.name "robot";
git commit -m "Release v$TAG";
git tag $TAG
git push --tags && git push
CHANGELOG="$CHANGELOG

Full Changelog: [$LAST_TAG...$TAG](https://github.com/ydb-platform/ydb-dotnet-sdk/compare/$LAST_TAG...$TAG)"

VERSION=$TAG
cd src
dotnet pack -c Release -o out /p:Version=$VERSION
gh release create $TAG -t "$TAG" --notes "$CHANGELOG"
dotnet nuget push out/Ydb.Sdk.$VERSION.nupkg  --skip-duplicate --api-key $NUGET_TOKEN --source https://api.nuget.org/v3/index.json
