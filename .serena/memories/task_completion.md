# Completion Checks

- For source changes, run `npm test` and `npm run build` from the repository root.
- If a change is isolated to one package, run that package's `npm test`/`npm run build` or the equivalent Lerna scoped command first, then broaden checks when practical.
- Before committing, inspect `git status --short --untracked-files=all`, `git diff`, and `git diff --cached`; confirm ignored dependencies, caches, local overrides, and generated `dist/` output are not staged.
- After committing, run `git status --short --branch` and inspect each commit with `git show --stat --oneline` plus the full commit message.