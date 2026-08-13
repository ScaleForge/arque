# Toolchain

- Runtime pin: Node.js `v20.18.0` from `.nvmrc`.
- Language: TypeScript; root `tsconfig.json` targets ES2017, uses CommonJS, Node module resolution, declarations, source maps, and strict library skipping.
- Package management: npm workspaces plus Lerna `9.0.7`; workspace packages are core, Kafka stream adapter, Mongo config adapter, and Mongo store adapter.
- Testing: Jest `30.2.0`, ts-jest `29.4.6`, Nx Jest preset integration.
- Build: package-local TypeScript builds invoked through Lerna; `rimraf` cleans `dist` first.
- Main external persistence/transport integrations: Mongoose `9.8.0`, KafkaJS `2.2.4`, FlatBuffers `24.3.25`, and MongoDB memory server for examples/tests.
- Root dependency overrides pin security-sensitive transitive dependencies such as `tar`, `axios`, `brace-expansion`, `@nx/devkit`, and `js-yaml`.