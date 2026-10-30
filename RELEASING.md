# Releasing

Releases are automated. lionturtle is the one published crate in this
workspace: its version lives in `lionturtle/Cargo.toml`, and `kvbench` is never
published. On every push to `main`, `.github/workflows/bump.yml`
("Auto Release") decides a version, tags it, creates a GitHub release, and
calls `.github/workflows/release.yml`, which publishes to crates.io over
**Trusted Publishing** (OIDC — there is no long-lived token secret).

## Day-to-day

Just merge to `main`. By default the version bumps by a **patch** and escalates
automatically (patch → minor → major) until `cargo-semver-checks` agrees the
level covers the public-API diff since the last published release — so a
breaking change cannot sneak out as a patch.

Control it via the head commit message (or PR title for squash merges):

- `#minor` / `#major` — force that bump level (the run fails if the level can't
  cover the API diff).
- `[skip release]` — don't release this push.

You can also trigger **Actions → Auto Release → Run workflow** and pick a level.

`workflow_dispatch` on **Release** re-runs a publish for an existing `vX.Y.Z`
tag whose publish never landed (it's a no-op if that version is already on
crates.io).

## One-time setup the owner must do

These are manual actions on external services; the workflows assume they are in
place.

### 1. Enable crates.io Trusted Publishing (required — no setup, no publish)

Trusted Publishing lets GitHub Actions publish to crates.io using a short-lived
OIDC token instead of a stored `CARGO_REGISTRY_TOKEN`. You configure it **after
the crate name exists on crates.io**. Two ways to get there:

- **Preferred:** do the very first `cargo publish` **manually** from your
  machine (`cargo login` with a personal token, then
  `cargo publish -p lionturtle`). This
  reserves the name and creates the crate. Then set up Trusted Publishing so all
  later releases are tokenless.
- Or configure a Trusted Publisher first and let the workflow's first-publish
  path do it; either works.

Then, on crates.io:

1. Sign in and open the crate: **crates.io → lionturtle → Settings →
   Trusted Publishing**.
2. **Add** a GitHub publisher:
   - Repository owner: `danthegoodman1`
   - Repository name: `lionturtle`
   - Workflow filename: `release.yml`
   - (Environment: leave blank unless you add a GitHub Environment gate.)
3. **Add a second publisher** identical to the first but with workflow filename
   `bump.yml`. This is required because the OIDC claim names the **top-level**
   workflow that started the run, and the auto-release path starts in
   `bump.yml` (which then calls `release.yml` as a reusable workflow).

No repository secrets are needed. Do **not** add a `CARGO_REGISTRY_TOKEN`
secret — the workflow mints a token per run via
`rust-lang/crates-io-auth-action`.

### 2. Repository settings

- **Settings → Actions → General → Workflow permissions:** allow workflows to
  **read and write** (the bump job pushes the release commit, tag, and GitHub
  release using the built-in `GITHUB_TOKEN`).
- If `main` is protected, allow `github-actions[bot]` to push the
  `chore(release): vX.Y.Z` commit and the tag (or the auto-release will fail at
  the push step).

## What runs where

- **bump.yml** (push to `main`): decide bump level → first-publish check →
  `cargo test --workspace` → escalate version with `cargo-semver-checks` → commit + tag +
  GitHub release → call `release.yml`.
- **release.yml** (tag push / called / manual): `fmt` + `clippy` + `cargo test`
  on Linux x86_64 and arm64 → verify tag matches `lionturtle/Cargo.toml` → skip
  if the version is already on crates.io → OIDC auth → `cargo publish -p lionturtle`.

## First release checklist

- [ ] `lionturtle/Cargo.toml` `version` is the intended first version (e.g. `0.1.0`).
- [ ] Crate name reserved on crates.io (manual first `cargo publish`, or via the
      first-publish path).
- [ ] Both Trusted Publishers added (`release.yml` **and** `bump.yml`).
- [ ] Workflow permissions set to read/write.
- [ ] Push to `main` (or run **Auto Release**) and watch the run.
