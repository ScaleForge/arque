# Project Conventions

- Package source is under `src/`; package tests are colocated under `src/**/*.test.ts` or, for Mongo store integration tests, under `packages/mongo-store-adapter/tests/`.
- Public package exports are assembled through each package's `src/index.ts`.
- Adapter implementations are class-based and expose lifecycle methods such as `init` and `close`; core aggregate behavior is class-based and uses typed options/handler maps.
- Package builds emit `dist/`, which is ignored; do not commit generated package output.
- ESLint uses TypeScript-aware rules: semicolons and single quotes are enforced through `@typescript-eslint`, multiline trailing commas are required, and unused arguments may be prefixed with `_`.
- Preserve existing package naming and independent package versioning; avoid unrelated formatting or refactors.