@AGENTS.md

<!-- AGENTS.md is the single source of truth for this repo (also read by other agents).
     Put repository knowledge there; keep only Claude Code–specific workflow notes below. -->

## Workflow notes for Claude Code

- Before calling a change done: build `src/YdbSdk.sln` and run the affected test project against a
  local YDB (see "Build and Test" above). If YDB is not running, say so — don't silently skip tests.
- Run `jb cleanupcode` with the "Custom Cleanup" profile on every solution you touched before
  committing; CI fails on any resulting diff. `dotnet format` is not what CI checks.
- Every user-visible change gets a CHANGELOG bullet in the right package (see "Conventions").
- When you add or rename a meter, instrument, attribute or span, update the metrics/tracing tables in
  AGENTS.md in the same change.
- Don't touch `publish*.yml`, `.github/scripts/publish*.sh` or version numbers unless asked.
