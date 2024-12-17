#!/bin/bash
set -euxo pipefail

CHANGELOG=$(cat $CHANGELOG_FILE | sed -e '/^## v.*$/,$d')
if [[ -z "$CHANGELOG" ]]
then
  echo "CHANGELOG empty"
  exit 1;
fi;

LAST_TAG=$(git tag | tail -n 1);
MAJOR=$(echo $LAST_TAG | sed -E 's/v([0-9]+)\..*/\1/');
MINOR=$(echo $LAST_TAG | sed -E 's/v[0-9]+\.([0-9]+)\..*/\1/');
PATCH=$(echo $LAST_TAG | sed -E 's/v[0-9]+\.[0-9]+\.([0-9]+)($|-rc[0-9]+)/\1/');
RC=0;

if [ "$RELEASE_CANDIDATE" = true ]
then
  RC=$(git tag | grep "v$MAJOR.$MINOR.$PATCH-rc" | wc -l | xargs || true); 
fi  
if [ "$VERSION_CHANGE" = "MINOR" ] && [ $RC = 0 ]
then
  MINOR=$((MINOR+1));
  PATCH=0;
fi;
if [ "$VERSION_CHANGE" = "PATCH" ] && [ $RC = 0 ]
then
  PATCH=$((PATCH+1));
fi;
if [ "$RELEASE_CANDIDATE" = true ]
then
  VERSION="$MAJOR.$MINOR.$PATCH-rc$RC";
else
  sed -e "s/Minor = [0-9]*/Minor = $MINOR/g" -i $VERSION_FILE
  sed -e "s/Patch = [0-9]*/Patch = $PATCH/g" -i $VERSION_FILE
  git add $VERSION_FILE;
  VERSION="$MAJOR.$MINOR.$PATCH";
fi;
TAG="v$VERSION"
echo "## v$VERSION" >> $CHANGELOG_FILE.tmp
cat $CHANGELOG_FILE >> $CHANGELOG_FILE.tmp
mv $CHANGELOG_FILE.tmp $CHANGELOG_FILE;
git add $CHANGELOG_FILE;
git config --global user.email "robot@umbrella";
git config --global user.name "robot";
git commit -m "Release v$VERSION";
git tag $TAG
git push --tags && git push
CHANGELOG="$CHANGELOG

Full Changelog: [$LAST_TAG...$TAG](https://github.com/ydb-platform/ydb-dotnet-sdk/compare/$LAST_TAG...$TAG)"

cd src
dotnet pack -c Release -o out /p:Version=$VERSION
gh release create $TAG -t "$TAG" --notes "$CHANGELOG"
dotnet nuget push out/Ydb.Sdk.$VERSION.nupkg  --skip-duplicate --api-key $NUGET_TOKEN --source https://api.nuget.org/v3/index.json

