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

## 5. Final Plan Review (Very Important)

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
