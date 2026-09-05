# S.T.A.R.E Release Process

The portal uses Semantic Versioning (`major.minor.patch`) and generates its
user-facing release history from Git commit metadata during every frontend
build.

## Version Rules

- `fix(portal): ...` and `perf(portal): ...` increment the patch version:
  `1.0.1` becomes `1.0.2`.
- `feat(portal): ...` increments the minor version: `1.0.1` becomes `1.1.0`.
- A `!` after the type or a `BREAKING CHANGE:` footer increments the major
  version: `1.1.0` becomes `2.0.0`.
- Documentation, test, build, CI, and automated market-data commits do not
  change the application version.

This follows standard SemVer instead of treating a version as a decimal. For
example, `1.10.0` is ten minor releases after `1.0.0`, while `1.1.10` is the
tenth patch of minor release `1.1`.

## User-Facing Notes

Every user-facing commit must use `feat`, `fix`, `perf`, or `revert`. The commit
subject becomes the default release note. Add one or more `Release-Note:` lines
to the commit body when clearer user language or multiple bullets are needed:

```text
feat(portal): add report exports

Release-Note: Download the current table as CSV.
Release-Note: Export the complete report snapshot as JSON.
```

`npm run release:generate` reads commits after the baseline in
`apps/web/release-baseline.json` and writes `apps/web/public/releases.json`.
The same command runs automatically before local development and production
builds. The generated file is intentionally ignored because Git history is its
source of truth.

The generator reads local history first and uses GitHub's paginated comparison
API when a deployment checkout is shallow. A build stops if neither source can
provide complete commit data, preventing an incomplete release list from being
published silently.

The build rejects unclassified commits after the release baseline so a change
cannot silently disappear. Use a conventional maintenance prefix such as
`docs:`, `test:`, `ci:`, or `chore:` for non-user-facing work. For an unusual
internal commit, add `Release-Note: none` to its body deliberately.

Users see this history from the **What's new** button. The button displays an
unread indicator when its latest version differs from the version last opened
in that browser.
