# Common Commands

- Install root dependencies: `npm install`.
- Run all package tests: `npm test`.
- Build all packages: `npm run build`.
- Run one workspace script: `npx lerna run test --scope <package-name>` or `npx lerna run build --scope <package-name>`.
- Inspect all changes, including untracked files: `git status --short --untracked-files=all`; tracked diffs: `git diff` and `git diff --cached`.
- Check Serena memory references from the repository root: `serena memories check`.
- Use Node 20.18.0 from `.nvmrc` before running package commands.