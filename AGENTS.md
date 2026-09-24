# AGENTS.md

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:

- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:

- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:

- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:

- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:

```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

For code changes, verify with:

```bash
npx nx run-many --target=typecheck,lint --skip-nx-cache
```

### Implementation Plan Specification

For any change involving code, configuration, data, or tests, make the plan specific enough that another engineer could implement it without reconstructing the approach. Include:

- **Objective:** State the user-visible or system behavior that must change.
- **Scope:** List the affected projects, files, and symbols. State what is explicitly out of scope.
- **Current behavior:** Describe the existing control flow or contract that the change relies on.
- **Target behavior:** Describe the exact inputs, conditions, outputs, side effects, and fallback behavior after the change.
- **Assumptions and decisions:** Record assumptions, unresolved questions, compatibility requirements, and the reason for each non-obvious design choice.
- **Implementation steps:** For every step, name the file and symbol to change, describe the code-level change, and identify what must remain unchanged.
- **Test plan:** Name the test files or targets, the scenarios to cover, and the commands to run. Include negative, boundary, permission, error, and backward-compatibility cases when applicable.
- **Verification criteria:** Define the exact evidence that proves each step is complete, including expected test, typecheck, lint, build, migration, or diff results.
- **Risks and follow-up:** Identify behavior that could regress, data or API compatibility concerns, and any work intentionally deferred.

Use this format for non-trivial work:

```
Objective: [behavior to change]
Scope: [projects/files/symbols]; out of scope: [explicit exclusions]
Assumptions: [known constraints and decisions]

Current behavior: [relevant existing flow or contract]
Target behavior: [exact new flow, conditions, outputs, and fallbacks]

Implementation:
1. [file and symbol] → [specific code change] → verify: [check]
2. [file and symbol] → [specific code change] → verify: [check]

Tests:
- [test file/target] → [scenario and assertion]
- Command: `[exact command]`

Success criteria:
- [observable behavior or verification result]
- [observable behavior or verification result]

Risks/follow-up: [known risks or explicitly deferred work]
```

Do not use vague steps such as "update the service" or "add tests". Name the method, argument, branch, event, schema field, migration, or configuration key being changed. If investigation changes the scope or design, revise the plan before continuing.

## 5. Final Plan Review

Before presenting or executing a final plan for a non-trivial task:

- Submit the user request, assumptions, gathered context, success criteria, and proposed plan to the `plan-review` subagent.
- Treat the subagent as read-only plan review: it must not implement changes, edit files, or run commands.
- Apply actionable feedback to the plan, then resubmit the revised plan for another review round.
- Stop when the reviewer returns `APPROVED` or after the tenth review round, whichever comes first.
- If the tenth round is reached without approval, proceed only with the latest plan and explicitly report unresolved reviewer feedback.

## 6. Prefer Serena for Code Work

- Serena must be the first tool used for source-code discovery, search, symbol lookup, references, targeted reads, and supported edits.
- Do not use Glob or Grep as an initial source-code search or merely for convenience. Start with Serena's symbol and pattern tools, including `get_symbols_overview`, `find_symbol`, `search_for_pattern`, and `find_referencing_symbols`.
- Use Serena's symbol-aware and content-editing tools to modify source files whenever they support the requested change.
- Use Glob or Grep only for non-code files, verification, or when Serena cannot identify or support the target; use the narrowest fallback search in that case.
- After any fallback discovery, return to Serena for source inspection and editing.
- Re-read or inspect the affected files after Serena edits and run relevant verification commands.
