---
name: fix-test
description: Run a specific test or discover failing tests from a GitHub PR and fix each with up to three failed attempts.
license: MIT
compatibility: opencode
---

# Fix Test

Accept a specific test target or a GitHub pull request URL. For a PR, discover the failing tests from CI and invoke this skill for each test. Run each test, fix failures, and retry until it passes or three failed attempts are made. If the test passes before any changes are made, treat it as potentially flaky and validate with repeat runs instead of stopping immediately.

## When to Use

- User asks to run a specific test and fix failures
- User provides a test file or directory to run
- User provides a GitHub PR URL whose failing tests need fixing
- User invokes the fix-test skill
- User says "fix test" or similar

## Workflow

1. Collect test target

   - If the user supplies a test file or directory, use that target and continue to step 2. Preserve any supplied test-name filter.
   - If the user supplies a GitHub PR URL:
     1. Parse the repository owner, repository name, and PR number. Use available GitHub tools or GitHub CLI to inspect PR metadata and obtain its current head SHA.
     2. Inspect checks, workflow runs, and failure logs for that head SHA. Identify specific failing test file paths and test case names from the logs, recording the supporting job/run information. Do not use failures from older commits or infer failed tests from changed files or a generic failed check.
     3. Deduplicate repeated failures for the same test across matrix jobs. If logs are inaccessible or truncated, or test paths are ambiguous, report the limitation and ask for the missing evidence or target. If checks are pending, no tests failed, or failures are only build, lint, or infrastructure errors, report that status rather than inventing test targets.
     4. Before running tests, verify that the local checkout matches the PR head. If it differs, stop and ask how to proceed; do not automatically switch branches or discard changes.
     5. For each unique failing test, explicitly invoke the `fix-test` skill with the resolved test file and a test-name filter when the runner supports it. Process targets sequentially. Pass the test target, not the PR URL, so the invocation follows the direct-target branch rather than recursively inspecting the PR.
     6. Apply steps 2–6 and their output expectations independently to each target. Each target has its own three-failure limit and five-consecutive-pass flaky validation. If one target reaches its limit, report it as unresolved and continue with the remaining identified targets. Summarize all target outcomes, then stop the PR-level workflow.
   - If neither a test target nor a GitHub PR URL is supplied, ask for one and stop.

2. Load related repository guidance

   - Follow `AGENTS.md` and any applicable project instructions. Inspect the relevant test and neighboring tests for fixture setup, mocks, teardown, assertions, and async patterns before changing them. Use Serena first for source-code discovery and supported edits.
   - If integration-test guidelines exist for the target, read and follow them before running or changing integration-style tests.

3. Run the test

   - Identify the owning workspace and its test script. Run the smallest command targeting the requested file or directory, preserving any test-name filter. From the repository root, for example: `npm test --workspace=@arque/core -- --runTestsByPath src/libs/aggregate-process.test.ts --runInBand`. Add `--testNamePattern='<name>'` when a case name is supplied. For a directory, use a Jest path pattern instead of `--runTestsByPath`.
   - Use the owning workspace script rather than root `npm test` so package-specific settings (such as the mongo-store adapter's `NODE_OPTIONS`) are retained. If the workspace has no test script, or the target cannot be resolved to a supported runner, report the blocker rather than inventing a command. Do not broaden the test target silently.

4. Analyze results

   - If the test passes without changes, treat it as potentially flaky. Re-run the same command until it has passed five consecutive times in total (including the initial pass) or fails.
   - If a repeat fails, analyze that failure and continue with the fix workflow. If the test passes after a fix, stop the target's retry loop.
   - Distinguish test failures from missing services, authentication, permissions, or other infrastructure blockers; report blockers rather than changing unrelated infrastructure.

5. Fix on failure

   - Fix the test or minimal related code without skipping tests, masking failures, or weakening assertions. Preserve unrelated changes.
   - Re-run the same targeted command after each fix. After a passing fix, run the owning package's tests without the target filter and `npx nx run-many --target=typecheck,lint --skip-nx-cache` as regression checks; report any regression failures separately.

6. Retry limit

   - Stop after a passing fix or three total failed targeted test runs per target, including the initial run and any failures during unchanged flaky validation. Successful unchanged-validation runs do not count as failures.

## Output Expectations

- For PR input, include a named Markdown link to the PR, supporting CI job/run information, the discovered test targets, and each target's final outcome or discovery blocker.
- Report the command and pass/fail result after each targeted run.
- If the test passed without changes, report that it was treated as potentially flaky and include the repeat-run result.
- If a fix was applied, summarize the change and affected file paths, plus package-test and typecheck/lint regression results.
- Stop each target after success or three failed runs. For PR input, continue through the remaining discovered targets before reporting the aggregate outcome.
