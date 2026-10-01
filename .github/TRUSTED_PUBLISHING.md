# NuGet Trusted Publishing

All three publish workflows authenticate with GitHub Actions OIDC using
`NuGet/login`. They obtain a temporary NuGet API key after the build and pass
it to the existing publish script as `NUGET_TOKEN`. The key is valid for one
hour. No long-lived NuGet API key is used.

The NuGet login profile is `robot-ydb-platform`. This public username is
specified directly in each workflow; no `NUGET_USER` variable is required.
The package owner is the NuGet organization `ydb-platform`, not the login
profile. The login account must have permission to publish its packages.

## One-time setup

1. Sign in to nuget.org as `robot-ydb-platform`. Open **Trusted Publishing**
   and create three policies with **Package owner** set to `ydb-platform`:

   | Repository owner | Repository | Workflow file | Package pattern |
   | --- | --- | --- | --- |
   | `ydb-platform` | `ydb-dotnet-sdk` | `publish.yml` | `Ydb.Sdk` |
   | `ydb-platform` | `ydb-dotnet-sdk` | `publish-ef.yml` | `EntityFrameworkCore.Ydb` |
   | `ydb-platform` | `ydb-dotnet-sdk` | `publish-otel.yml` | `Ydb.Sdk.OpenTelemetry` |

   Use only the workflow filename, without `.github/workflows/`. Leave
   **Environment** empty: these workflows do not use GitHub environments.
   Restrict each policy to its exact package ID and select **Push only new
   package versions**. Do not enable unlisting or relisting.

2. Merge the workflow changes and run the appropriate workflow manually for
   the next intended release. Verify that **NuGet login** succeeds and that
   the new package version appears on nuget.org. Running a publish workflow
   performs a real release; it is not a dry run.

3. After successful OIDC publication for every workflow and repository using
   the old key, revoke it on nuget.org and delete the unused GitHub Actions
   secret `YDB_PLATFORM_NUGET_TOKEN`. Other repositories may still depend on
   this key; migrating this repository alone is not enough to revoke it.

The GitHub bot token `YDB_PLATFORM_BOT_TOKEN_REPO` is separate from NuGet
authentication. The SDK and EF workflows still use it to commit the changelog,
push release tags, and create GitHub releases.

## Reference

[NuGet Trusted Publishing](https://learn.microsoft.com/nuget/nuget-org/trusted-publishing)
