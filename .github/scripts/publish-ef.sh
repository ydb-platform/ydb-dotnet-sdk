#!/bin/bash
set -euxo pipefail

CHANGELOG=$(cat $CHANGELOG_FILE | sed -e '/^## v.*$/,$d')
if [[ -z "$CHANGELOG" ]]
then
  echo "CHANGELOG empty"
  exit 1;
fi;

LAST_EF_TAG=$(git tag --sort=-creatordate --list 'ef-*' | head -n 1 | cut -d '-' -f 2);
LAST_EF_TAG=${LAST_EF_TAG:-v0.0.0}
MAJOR=$(echo $LAST_EF_TAG | sed -E 's/v([0-9]+)\..*/\1/');
MINOR=$(echo $LAST_EF_TAG | sed -E 's/v[0-9]+\.([0-9]+)\..*/\1/');
PATCH=$(echo $LAST_EF_TAG | sed -E 's/v[0-9]+\.[0-9]+\.([0-9]+)($|-rc[0-9]+)/\1/');
RC=0;

if [ "$RELEASE_CANDIDATE" = true ]
then
  RC=$(git tag | grep "ef-v$MAJOR.$MINOR.$PATCH-rc" | wc -l | xargs || true); 
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
  VERSION="$MAJOR.$MINOR.$PATCH";
fi;

TAG="v$VERSION"
echo -e "## $TAG\n" >> $CHANGELOG_FILE.tmp
cat $CHANGELOG_FILE >> $CHANGELOG_FILE.tmp
mv $CHANGELOG_FILE.tmp $CHANGELOG_FILE;
git add $CHANGELOG_FILE;
git config --global user.email "robot@umbrella";
git config --global user.name "robot";
git commit -m "Release EF $TAG";
git tag "ef-$TAG"
git push --tags && git push
CHANGELOG="$CHANGELOG

Full Changelog: [$LAST_EF_TAG...$TAG](https://github.com/ydb-platform/ydb-dotnet-sdk/compare/$LAST_EF_TAG...$TAG)"

cd src/EFCore.Ydb/src
dotnet pack -c Release -o out /p:Version=$VERSION
gh release create "ef-$TAG" -t "ef-$TAG" --notes "$CHANGELOG"
dotnet nuget push out/EntityFrameworkCore.Ydb.$VERSION.nupkg  --skip-duplicate --api-key $NUGET_TOKEN --source https://api.nuget.org/v3/index.json
