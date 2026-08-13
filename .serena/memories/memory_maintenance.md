# Memory Maintenance

## Discovery Model

- Core principle: progressive discovery through references, building a graph of memories.
- Initially, agents are provided with the list of all memories (names only). Agents should read `mem:core` as the top-level entry point.
- Keep `mem:core` as the project map and point to focused memories for toolchain, conventions, commands, and completion checks.
- Use topic folders for clearly distinct modules or domains; avoid pushing module-specific details into the root memory.

## Style

- Write dense agent notes, not prose docs.
- Prefer terse bullets and durable, non-obvious facts.
- Avoid generic language/framework knowledge, one-off task notes, and volatile line-level details.
- Add/update memories only when they reduce future rediscovery.

## Maintenance Actions

- Rename memories with Serena's memory rename tool so references update automatically.
- Check for stale references with `serena memories check` from the project root.