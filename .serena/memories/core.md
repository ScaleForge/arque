# Arque Core Map

- TypeScript monorepo managed by Lerna workspaces; packages live under `packages/`.
- Core domain package: `packages/core`; public entrypoint `packages/core/src/index.ts`; aggregate/event-sourcing behavior is centered in `packages/core/src/libs/aggregate.ts`.
- Adapter packages: `packages/kafka-stream-adapter`, `packages/mongo-config-adapter`, and `packages/mongo-store-adapter`; each exposes its adapter from `src/index.ts`.
- Root scripts: `npm test` runs `lerna run test`; `npm run build` runs `lerna run build`.
- Read package-specific scripts and behavior from each package's `package.json` before changing verification or release workflows.

For versions and package tooling, read `mem:tech_stack`. For style and package patterns, read `mem:conventions`. For completion commands, read `mem:task_completion` and `mem:suggested_commands`.